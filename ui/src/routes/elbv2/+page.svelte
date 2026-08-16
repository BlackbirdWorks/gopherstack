<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { multiRegionList } from '$lib/multi-region';
	import { isAllRegions, currentRegion } from '$lib/region.svelte';
	import RegionChip from '$lib/components/RegionChip.svelte';
	import WriteRegionHint from '$lib/components/WriteRegionHint.svelte';
	import { getELBv2Client } from '$lib/aws-client';
	import {
		DescribeLoadBalancersCommand,
		DescribeTargetGroupsCommand,
		DescribeListenersCommand,
		DescribeTargetHealthCommand,
		DescribeRulesCommand,
		DescribeListenerCertificatesCommand,
		DescribeTrustStoresCommand,
		DescribeTrustStoreRevocationsCommand,
		CreateLoadBalancerCommand,
		DeleteLoadBalancerCommand,
		CreateTargetGroupCommand,
		DeleteTargetGroupCommand,
		CreateListenerCommand,
		DeleteListenerCommand,
		CreateTrustStoreCommand,
		DeleteTrustStoreCommand,
		CreateRuleCommand,
		ModifyRuleCommand,
		DeleteRuleCommand,
		SetRulePrioritiesCommand,
		DescribeTargetGroupAttributesCommand,
		ModifyTargetGroupAttributesCommand,
		RegisterTargetsCommand,
		DeregisterTargetsCommand,
		AddListenerCertificatesCommand,
		RemoveListenerCertificatesCommand,
		ModifyListenerCommand,
		ProtocolEnum,
		TargetTypeEnum,
		type LoadBalancer,
		type TargetGroup,
		type Listener,
		type Rule,
		type Certificate,
		type RuleCondition,
		type TrustStore,
		type TrustStoreRevocation
	} from '@aws-sdk/client-elastic-load-balancing-v2';
	import { toast } from 'svelte-sonner';
	import {
		Network,
		Search,
		RefreshCw,
		Plus,
		Trash2,
		CheckCircle,
		XCircle,
		ChevronRight,
		ChevronDown,
		Server,
		Shield,
		List,
		Lock,
		Copy,
		Package,
		BarChart2,
		BookOpen
	} from 'lucide-svelte';

	const elb = regionalClient(getELBv2Client);

	// Placeholder S3 values for the trust store CA bundle.
	const TRUST_STORE_CA_S3_BUCKET = 'gopherstack-demo';
	const TRUST_STORE_CA_S3_KEY = 'certs.pem';

	// Every row carries the region its Describe*Command call was made against.
	// Row actions (delete/detail/register/etc) must use THIS region, not the
	// page's shared `elb()` client -- in All mode the same name/ARN can
	// legitimately exist in two different regions.
	type Regioned<T> = T & { region: string };

	type Tab = 'lbs' | 'targets' | 'listeners' | 'truststores' | 'metrics' | 'docs';

	let loading = $state(false);
	let activeTab = $state<Tab>('lbs');
	let searchQuery = $state('');

	// ── Load Balancers ──────────────────────────────────────────────────────────
	let loadBalancers = $state<Regioned<LoadBalancer>[]>([]);
	let selectedLB = $state<Regioned<LoadBalancer> | null>(null);
	let lbListeners = $state<Listener[]>([]);
	let loadingListeners = $state(false);
	let showCreateModal = $state(false);
	let creating = $state(false);
	let newLBName = $state('');
	let newLBType = $state<'application' | 'network' | 'gateway'>('application');
	let newLBScheme = $state<'internet-facing' | 'internal'>('internet-facing');
	let newLBSubnets = $state('');

	// ── Target Groups ───────────────────────────────────────────────────────────
	let targetGroups = $state<Regioned<TargetGroup>[]>([]);
	let selectedTG = $state<Regioned<TargetGroup> | null>(null);
	let tgHealth = $state<Array<{ Target?: { Id?: string; Port?: number }; TargetHealth?: { State?: string; Description?: string } }>>([]);
	let loadingHealth = $state(false);

	// Target-group stickiness + register
	let tgStickinessEnabled = $state(false);
	let tgStickinessDuration = $state(86400);
	let savingTgAttrs = $state(false);
	let newTargetId = $state('');
	let newTargetPort = $state('80');
	let registeringTarget = $state(false);
	let showCreateTGModal = $state(false);
	let newTGName = $state('');
	let newTGType = $state<'instance' | 'ip' | 'lambda'>('instance');
	let newTGProtocol = $state('HTTP');
	let newTGPort = $state('80');
	let creatingTG = $state(false);

	// ── Listeners ───────────────────────────────────────────────────────────────
	let allListeners = $state<Regioned<Listener>[]>([]);
	let loadingAllListeners = $state(false);
	let selectedListener = $state<Regioned<Listener> | null>(null);
	let listenerRules = $state<Rule[]>([]);
	let loadingRules = $state(false);
	let listenerCerts = $state<Certificate[]>([]);
	let loadingCerts = $state(false);
	let filterByLB = $state('');
	let showCreateListenerModal = $state(false);
	let newListenerLBArn = $state('');
	let newListenerProtocol = $state('HTTP');
	let newListenerPort = $state('80');
	let newListenerSslPolicy = $state('');
	let creatingListener = $state(false);

	// cert management
	let showAddCertModal = $state(false);
	let newCertArn = $state('');
	let addingCert = $state(false);

	// rule condition editor
	let showRuleEditor = $state(false);
	let editingRule = $state<Rule | null>(null);
	let ruleCondField = $state('host-header');
	let ruleCondValues = $state('');
	let ruleCondHeaderName = $state('');
	let ruleActionTGArn = $state('');
	let rulePriority = $state('');
	let savingRule = $state(false);

	// ── Trust Stores ────────────────────────────────────────────────────────────
	let trustStores = $state<Regioned<TrustStore>[]>([]);
	let loadingTrustStores = $state(false);
	let selectedTrustStore = $state<Regioned<TrustStore> | null>(null);
	let trustStoreRevocations = $state<TrustStoreRevocation[]>([]);
	let loadingRevocations = $state(false);
	let showCreateTSModal = $state(false);
	let newTSName = $state('');
	let creatingTS = $state(false);

	// ── derived ─────────────────────────────────────────────────────────────────
	const filteredLBs = $derived(
		loadBalancers.filter(
			(lb) =>
				(lb.LoadBalancerName ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(lb.DNSName ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const filteredTGs = $derived(
		targetGroups.filter(
			(tg) =>
				(tg.TargetGroupName ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(tg.Protocol ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const filteredListeners = $derived(
		allListeners
			.filter((l) =>
				(filterByLB === '' || l.LoadBalancerArn === filterByLB) &&
				(
					(l.Protocol ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
					String(l.Port ?? '').includes(searchQuery)
				)
			)
	);

	const metrics = $derived({
		lbCount: loadBalancers.length,
		albCount: loadBalancers.filter((lb) => lb.Type === 'application').length,
		nlbCount: loadBalancers.filter((lb) => lb.Type === 'network').length,
		gwbCount: loadBalancers.filter((lb) => lb.Type === 'gateway').length,
		tgCount: targetGroups.length,
		tgInstance: targetGroups.filter((t) => t.TargetType === 'instance').length,
		tgIp: targetGroups.filter((t) => t.TargetType === 'ip').length,
		tgLambda: targetGroups.filter((t) => t.TargetType === 'lambda').length,
		listenerCount: allListeners.length,
		tsCount: trustStores.length
	});

	// ── helpers ─────────────────────────────────────────────────────────────────
	function lbStateBadge(state?: string) {
		if (state === 'active') return 'text-green-700 bg-green-100 dark:text-green-300 dark:bg-green-900';
		if (state === 'provisioning') return 'text-yellow-700 bg-yellow-100 dark:text-yellow-300 dark:bg-yellow-900';
		if (state === 'failed') return 'text-red-700 bg-red-100 dark:text-red-300 dark:bg-red-900';
		return 'text-muted-foreground bg-muted';
	}

	function healthBadge(state?: string) {
		if (state === 'healthy') return 'text-green-700 bg-green-100 dark:text-green-300 dark:bg-green-900';
		if (state === 'unhealthy') return 'text-red-700 bg-red-100 dark:text-red-300 dark:bg-red-900';
		return 'text-muted-foreground bg-muted';
	}

	function lbTypeBadge(type?: string) {
		if (type === 'application') return 'bg-teal-100 text-teal-800 dark:bg-teal-900 dark:text-teal-200';
		if (type === 'network') return 'bg-purple-100 text-purple-800 dark:bg-purple-900 dark:text-purple-200';
		if (type === 'gateway') return 'bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-200';
		return 'bg-muted text-muted-foreground';
	}

	function tgTypeBadge(type?: string) {
		if (type === 'instance') return 'bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200';
		if (type === 'ip') return 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200';
		if (type === 'lambda') return 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200';
		return 'bg-muted text-muted-foreground';
	}

	function conditionLabel(cond: RuleCondition): string {
		const field = cond.Field ?? '?';
		switch (field) {
			case 'host-header': {
				const vals = cond.HostHeaderConfig?.Values ?? [];
				return `Host: ${vals.join(', ')}`;
			}
			case 'path-pattern': {
				const vals = cond.PathPatternConfig?.Values ?? [];
				return `Path: ${vals.join(', ')}`;
			}
			case 'http-header': {
				const name = cond.HttpHeaderConfig?.HttpHeaderName ?? '';
				const vals = cond.HttpHeaderConfig?.Values ?? [];
				return `Header ${name}: ${vals.join(', ')}`;
			}
			case 'http-request-method': {
				const vals = cond.HttpRequestMethodConfig?.Values ?? [];
				return `Method: ${vals.join(', ')}`;
			}
			case 'query-string': {
				const pairs = (cond.QueryStringConfig?.Values ?? []).map(
					(p) => `${p.Key ?? '*'}=${p.Value}`
				);
				return `Query: ${pairs.join(' & ')}`;
			}
			case 'source-ip': {
				const vals = cond.SourceIpConfig?.Values ?? [];
				return `SourceIP: ${vals.join(', ')}`;
			}
			default:
				return field;
		}
	}

	function conditionBadgeColor(field?: string): string {
		switch (field) {
			case 'host-header': return 'bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200';
			case 'path-pattern': return 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200';
			case 'http-header': return 'bg-purple-100 text-purple-800 dark:bg-purple-900 dark:text-purple-200';
			case 'http-request-method': return 'bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-200';
			case 'query-string': return 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200';
			case 'source-ip': return 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200';
			default: return 'bg-muted text-muted-foreground';
		}
	}

	function copyToClipboard(text: string, label: string) {
		navigator.clipboard.writeText(text).then(
			() => toast.success(`${label} copied`),
			() => toast.error('Copy failed')
		);
	}

	// ── data loaders ────────────────────────────────────────────────────────────
	// Fans a Describe*Command out across every region with data in All mode;
	// single-region mode issues exactly one call, tagged with currentRegion().
	async function loadRegioned<TResponse, TItem>(
		label: string,
		regionCall: (region: string) => Promise<TResponse>,
		extractItems: (r: TResponse) => TItem[],
		assign: (items: Regioned<TItem>[]) => void
	) {
		const result = await multiRegionList(regionCall, extractItems);
		assign(result.items.map(({ region, item }) => ({ ...item, region }) as Regioned<TItem>));
		if (result.errors.length > 0) toast.error(`Failed to load ${label} from ${result.errors.length} region(s)`);
	}

	async function loadLoadBalancers() {
		loading = true;
		try {
			await loadRegioned('load balancers',
				(region) => getELBv2Client(region).send(new DescribeLoadBalancersCommand({ PageSize: 100 })),
				(r) => r.LoadBalancers ?? [],
				(items) => loadBalancers = items);
		} catch (e) {
			toast.error(`Failed to load load balancers: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function loadTargetGroups() {
		loading = true;
		try {
			await loadRegioned('target groups',
				(region) => getELBv2Client(region).send(new DescribeTargetGroupsCommand({ PageSize: 100 })),
				(r) => r.TargetGroups ?? [],
				(items) => targetGroups = items);
		} catch (e) {
			toast.error(`Failed to load target groups: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function loadAllListeners() {
		loadingAllListeners = true;
		allListeners = [];
		try {
			await loadRegioned('listeners',
				(region) => getELBv2Client(region).send(new DescribeListenersCommand({})),
				(r) => r.Listeners ?? [],
				(items) => allListeners = items);
		} catch (e) {
			toast.error(`Failed to load listeners: ${e}`);
		} finally {
			loadingAllListeners = false;
		}
	}

	async function loadTrustStores() {
		loadingTrustStores = true;
		trustStores = [];
		try {
			await loadRegioned('trust stores',
				(region) => getELBv2Client(region).send(new DescribeTrustStoresCommand({})),
				(r) => r.TrustStores ?? [],
				(items) => trustStores = items);
		} catch (e) {
			toast.error(`Failed to load trust stores: ${e}`);
		} finally {
			loadingTrustStores = false;
		}
	}

	async function viewLBListeners(lb: Regioned<LoadBalancer>) {
		selectedLB = lb;
		loadingListeners = true;
		lbListeners = [];
		try {
			const res = await getELBv2Client(lb.region).send(
				new DescribeListenersCommand({ LoadBalancerArn: lb.LoadBalancerArn })
			);
			lbListeners = res.Listeners ?? [];
		} catch (e) {
			toast.error(`Failed to load listeners: ${e}`);
		} finally {
			loadingListeners = false;
		}
	}

	async function viewTGHealth(tg: Regioned<TargetGroup>) {
		selectedTG = tg;
		loadingHealth = true;
		tgHealth = [];
		newTargetId = '';
		newTargetPort = String(tg.Port ?? 80);
		try {
			const res = await getELBv2Client(tg.region).send(
				new DescribeTargetHealthCommand({ TargetGroupArn: tg.TargetGroupArn })
			);
			tgHealth = (res.TargetHealthDescriptions ?? []).map((d) => ({
				Target: d.Target
					? { Id: d.Target.Id, Port: d.Target.Port }
					: undefined,
				TargetHealth: d.TargetHealth
					? { State: d.TargetHealth.State, Description: d.TargetHealth.Description }
					: undefined
			}));
			await loadTgAttributes(tg);
		} catch (e) {
			toast.error(`Failed to load target health: ${e}`);
		} finally {
			loadingHealth = false;
		}
	}

	async function loadTgAttributes(tg: Regioned<TargetGroup>) {
		try {
			const res = await getELBv2Client(tg.region).send(new DescribeTargetGroupAttributesCommand({ TargetGroupArn: tg.TargetGroupArn }));
			const attrs = res.Attributes ?? [];
			tgStickinessEnabled = attrs.find((a) => a.Key === 'stickiness.enabled')?.Value === 'true';
			const dur = attrs.find((a) => a.Key === 'stickiness.lb_cookie.duration_seconds')?.Value;
			tgStickinessDuration = dur ? Number(dur) : 86400;
		} catch (e) {
			toast.error(`Failed to load target-group attributes: ${e}`);
		}
	}

	async function saveTgStickiness() {
		if (!selectedTG?.TargetGroupArn) return;
		savingTgAttrs = true;
		try {
			await getELBv2Client(selectedTG.region).send(new ModifyTargetGroupAttributesCommand({
				TargetGroupArn: selectedTG.TargetGroupArn,
				Attributes: [
					{ Key: 'stickiness.enabled', Value: String(tgStickinessEnabled) },
					{ Key: 'stickiness.type', Value: 'lb_cookie' },
					{ Key: 'stickiness.lb_cookie.duration_seconds', Value: String(tgStickinessDuration) }
				]
			}));
			toast.success('Stickiness settings saved');
		} catch (e) {
			toast.error(`Failed to save stickiness: ${e}`);
		} finally {
			savingTgAttrs = false;
		}
	}

	async function registerTarget() {
		if (!selectedTG?.TargetGroupArn || !newTargetId.trim()) return;
		registeringTarget = true;
		try {
			await getELBv2Client(selectedTG.region).send(new RegisterTargetsCommand({
				TargetGroupArn: selectedTG.TargetGroupArn,
				Targets: [{ Id: newTargetId.trim(), Port: newTargetPort ? Number(newTargetPort) : undefined }]
			}));
			toast.success(`Target "${newTargetId}" registered`);
			newTargetId = '';
			await viewTGHealth(selectedTG);
		} catch (e) {
			toast.error(`Failed to register target: ${e}`);
		} finally {
			registeringTarget = false;
		}
	}

	async function deregisterTarget(id: string | undefined, port: number | undefined) {
		if (!selectedTG?.TargetGroupArn || !id) return;
		try {
			await getELBv2Client(selectedTG.region).send(new DeregisterTargetsCommand({
				TargetGroupArn: selectedTG.TargetGroupArn,
				Targets: [{ Id: id, Port: port }]
			}));
			toast.success(`Target "${id}" deregistered`);
			await viewTGHealth(selectedTG);
		} catch (e) {
			toast.error(`Failed to deregister target: ${e}`);
		}
	}

	async function reorderRule(rule: Rule, direction: -1 | 1) {
		if (!selectedListener || rule.IsDefault) return;
		const nonDefault = listenerRules
			.filter((r) => !r.IsDefault)
			.toSorted((a, b) => (parseInt(a.Priority ?? '0', 10) || 0) - (parseInt(b.Priority ?? '0', 10) || 0));
		const idx = nonDefault.findIndex((r) => r.RuleArn === rule.RuleArn);
		const swapIdx = idx + direction;
		if (idx < 0 || swapIdx < 0 || swapIdx >= nonDefault.length) return;
		const a = nonDefault[idx];
		const b = nonDefault[swapIdx];
		const aPrio = parseInt(a.Priority ?? '0', 10);
		const bPrio = parseInt(b.Priority ?? '0', 10);
		try {
			await getELBv2Client(selectedListener.region).send(new SetRulePrioritiesCommand({
				RulePriorities: [
					{ RuleArn: a.RuleArn, Priority: bPrio },
					{ RuleArn: b.RuleArn, Priority: aPrio }
				]
			}));
			toast.success('Rule priority updated');
			await loadListenerRules(selectedListener);
		} catch (e) {
			toast.error(`Failed to reorder rule: ${e}`);
		}
	}

	async function selectListener(listener: Regioned<Listener>) {
		selectedListener = listener;
		await Promise.all([loadListenerRules(listener), loadListenerCerts(listener)]);
	}

	async function loadListenerRules(listener: Regioned<Listener>) {
		loadingRules = true;
		listenerRules = [];
		try {
			const res = await getELBv2Client(listener.region).send(
				new DescribeRulesCommand({ ListenerArn: listener.ListenerArn })
			);
			listenerRules = res.Rules ?? [];
		} catch (e) {
			toast.error(`Failed to load rules: ${e}`);
		} finally {
			loadingRules = false;
		}
	}

	async function loadListenerCerts(listener: Regioned<Listener>) {
		loadingCerts = true;
		listenerCerts = [];
		try {
			const res = await getELBv2Client(listener.region).send(
				new DescribeListenerCertificatesCommand({ ListenerArn: listener.ListenerArn })
			);
			listenerCerts = res.Certificates ?? [];
		} catch (e) {
			toast.error(`Failed to load certificates: ${e}`);
		} finally {
			loadingCerts = false;
		}
	}

	async function addCert() {
		if (!selectedListener?.ListenerArn || !newCertArn.trim()) return;
		addingCert = true;
		try {
			await getELBv2Client(selectedListener.region).send(
				new AddListenerCertificatesCommand({
					ListenerArn: selectedListener.ListenerArn,
					Certificates: [{ CertificateArn: newCertArn.trim() }]
				})
			);
			toast.success('Certificate added');
			newCertArn = '';
			showAddCertModal = false;
			await loadListenerCerts(selectedListener);
		} catch (e) {
			toast.error(`Failed to add certificate: ${e}`);
		} finally {
			addingCert = false;
		}
	}

	async function removeCert(certArn: string) {
		if (!selectedListener?.ListenerArn) return;
		const confirmed = await confirmDestructive({
			title: 'Remove Certificate',
			message: `Remove certificate ${certArn}?`
		});
		if (!confirmed) return;
		try {
			await getELBv2Client(selectedListener.region).send(
				new RemoveListenerCertificatesCommand({
					ListenerArn: selectedListener.ListenerArn,
					Certificates: [{ CertificateArn: certArn }]
				})
			);
			toast.success('Certificate removed');
			await loadListenerCerts(selectedListener);
		} catch (e) {
			toast.error(`Failed to remove certificate: ${e}`);
		}
	}

	function openCreateRule() {
		editingRule = null;
		ruleCondField = 'host-header';
		ruleCondValues = '';
		ruleCondHeaderName = '';
		ruleActionTGArn = '';
		rulePriority = '';
		showRuleEditor = true;
	}

	function openEditRule(rule: Rule) {
		editingRule = rule;
		const firstCond = rule.Conditions?.[0];
		ruleCondField = firstCond?.Field ?? 'host-header';
		const firstAction = rule.Actions?.[0];
		ruleActionTGArn = firstAction?.TargetGroupArn ?? '';
		rulePriority = rule.Priority ? String(rule.Priority) : '';

		if (ruleCondField === 'host-header') {
			ruleCondValues = (firstCond?.HostHeaderConfig?.Values ?? []).join(', ');
		} else if (ruleCondField === 'path-pattern') {
			ruleCondValues = (firstCond?.PathPatternConfig?.Values ?? []).join(', ');
		} else if (ruleCondField === 'http-header') {
			ruleCondHeaderName = firstCond?.HttpHeaderConfig?.HttpHeaderName ?? '';
			ruleCondValues = (firstCond?.HttpHeaderConfig?.Values ?? []).join(', ');
		} else if (ruleCondField === 'http-request-method') {
			ruleCondValues = (firstCond?.HttpRequestMethodConfig?.Values ?? []).join(', ');
		} else if (ruleCondField === 'source-ip') {
			ruleCondValues = (firstCond?.SourceIpConfig?.Values ?? []).join(', ');
		}
		showRuleEditor = true;
	}

	function buildConditionForSave(): RuleCondition {
		const values = ruleCondValues.split(',').map((v) => v.trim()).filter(Boolean);
		switch (ruleCondField) {
			case 'host-header':
				return { Field: 'host-header', HostHeaderConfig: { Values: values } };
			case 'path-pattern':
				return { Field: 'path-pattern', PathPatternConfig: { Values: values } };
			case 'http-header':
				return { Field: 'http-header', HttpHeaderConfig: { HttpHeaderName: ruleCondHeaderName, Values: values } };
			case 'http-request-method':
				return { Field: 'http-request-method', HttpRequestMethodConfig: { Values: values } };
			case 'source-ip':
				return { Field: 'source-ip', SourceIpConfig: { Values: values } };
			default:
				return { Field: ruleCondField };
		}
	}

	async function saveRule() {
		if (!selectedListener?.ListenerArn) return;
		savingRule = true;
		try {
			const condition = buildConditionForSave();
			const actions = ruleActionTGArn ? [{ Type: 'forward' as const, TargetGroupArn: ruleActionTGArn }] : [];

			if (editingRule?.RuleArn) {
				await getELBv2Client(selectedListener.region).send(
					new ModifyRuleCommand({
						RuleArn: editingRule.RuleArn,
						Conditions: [condition],
						Actions: actions.length > 0 ? actions : undefined
					})
				);
				toast.success('Rule updated');
			} else {
				const priority = rulePriority.trim() ? Number(rulePriority.trim()) : Date.now() % 50000;
				await getELBv2Client(selectedListener.region).send(
					new CreateRuleCommand({
						ListenerArn: selectedListener.ListenerArn,
						Priority: priority,
						Conditions: [condition],
						Actions: actions
					})
				);
				toast.success('Rule created');
			}
			showRuleEditor = false;
			await loadListenerRules(selectedListener);
		} catch (e) {
			toast.error(`Failed to save rule: ${e}`);
		} finally {
			savingRule = false;
		}
	}

	async function deleteRule(rule: Rule) {
		if (!rule.RuleArn || !selectedListener) return;
		const confirmed = await confirmDestructive({
			title: 'Delete Rule',
			message: `Delete rule with priority ${rule.Priority}?`
		});
		if (!confirmed) return;
		try {
			await getELBv2Client(selectedListener.region).send(new DeleteRuleCommand({ RuleArn: rule.RuleArn }));
			toast.success('Rule deleted');
			await loadListenerRules(selectedListener);
		} catch (e) {
			toast.error(`Failed to delete rule: ${e}`);
		}
	}

	async function viewTrustStoreRevocations(ts: Regioned<TrustStore>) {
		selectedTrustStore = ts;
		loadingRevocations = true;
		trustStoreRevocations = [];
		try {
			const res = await getELBv2Client(ts.region).send(
				new DescribeTrustStoreRevocationsCommand({ TrustStoreArn: ts.TrustStoreArn })
			);
			trustStoreRevocations = res.TrustStoreRevocations ?? [];
		} catch (e) {
			toast.error(`Failed to load revocations: ${e}`);
		} finally {
			loadingRevocations = false;
		}
	}

	async function deleteLB(lb: Regioned<LoadBalancer>) {
		if (!lb.LoadBalancerArn) return;
		const confirmed = await confirmDestructive({
			title: 'Delete Load Balancer',
			message: `Delete load balancer "${lb.LoadBalancerName}"? All listeners and rules will be removed.`
		});
		if (!confirmed) return;
		try {
			await getELBv2Client(lb.region).send(new DeleteLoadBalancerCommand({ LoadBalancerArn: lb.LoadBalancerArn }));
			toast.success(`Deleting "${lb.LoadBalancerName}"…`);
			await loadLoadBalancers();
		} catch (e) {
			toast.error(`Failed to delete: ${e}`);
		}
	}

	async function createLB() {
		if (!newLBName.trim() || !newLBSubnets.trim()) {
			toast.error('Name and subnets are required');
			return;
		}
		creating = true;
		try {
			await elb().send(
				new CreateLoadBalancerCommand({
					Name: newLBName.trim(),
					Type: newLBType,
					Scheme: newLBScheme,
					Subnets: newLBSubnets.split(',').map((s) => s.trim())
				})
			);
			toast.success(`Load balancer "${newLBName}" created`);
			showCreateModal = false;
			newLBName = '';
			newLBSubnets = '';
			await loadLoadBalancers();
		} catch (e) {
			toast.error(`Failed to create load balancer: ${e}`);
		} finally {
			creating = false;
		}
	}

	async function createTG() {
		if (!newTGName.trim()) { toast.error('Name is required'); return; }
		creatingTG = true;
		try {
			await elb().send(new CreateTargetGroupCommand({
				Name: newTGName.trim(),
				TargetType: newTGType as TargetTypeEnum,
				Protocol: newTGProtocol as ProtocolEnum,
				Port: parseInt(newTGPort, 10)
			}));
			toast.success(`Target group "${newTGName}" created`);
			showCreateTGModal = false;
			newTGName = '';
			await loadTargetGroups();
		} catch (e) {
			toast.error(`Failed to create target group: ${e}`);
		} finally {
			creatingTG = false;
		}
	}

	async function deleteTG(tg: Regioned<TargetGroup>) {
		if (!tg.TargetGroupArn) return;
		const confirmed = await confirmDestructive({
			title: 'Delete Target Group',
			message: `Delete target group "${tg.TargetGroupName}"?`
		});
		if (!confirmed) return;
		try {
			await getELBv2Client(tg.region).send(new DeleteTargetGroupCommand({ TargetGroupArn: tg.TargetGroupArn }));
			toast.success(`Deleted "${tg.TargetGroupName}"`);
			if (selectedTG && selectedTG.TargetGroupArn === tg.TargetGroupArn && selectedTG.region === tg.region) selectedTG = null;
			await loadTargetGroups();
		} catch (e) {
			toast.error(`Failed to delete: ${e}`);
		}
	}

	// A listener attaches to a specific load balancer, so it must be created
	// in THAT load balancer's region, not the page's default write region.
	async function createListener() {
		if (!newListenerLBArn) { toast.error('Select a load balancer'); return; }
		const lb = loadBalancers.find((l) => l.LoadBalancerArn === newListenerLBArn);
		if (!lb) { toast.error('Selected load balancer not found'); return; }
		creatingListener = true;
		try {
			await getELBv2Client(lb.region).send(new CreateListenerCommand({
				LoadBalancerArn: newListenerLBArn,
				Protocol: newListenerProtocol as ProtocolEnum,
				Port: parseInt(newListenerPort, 10),
				SslPolicy: newListenerSslPolicy || undefined,
				DefaultActions: [{ Type: 'fixed-response', FixedResponseConfig: { StatusCode: '200' } }]
			}));
			toast.success('Listener created');
			showCreateListenerModal = false;
			newListenerLBArn = '';
			newListenerSslPolicy = '';
			await loadAllListeners();
		} catch (e) {
			toast.error(`Failed to create listener: ${e}`);
		} finally {
			creatingListener = false;
		}
	}

	async function deleteListener(listener: Regioned<Listener>) {
		if (!listener.ListenerArn) return;
		const confirmed = await confirmDestructive({
			title: 'Delete Listener',
			message: `Delete ${listener.Protocol}:${listener.Port} listener and all its rules?`
		});
		if (!confirmed) return;
		try {
			await getELBv2Client(listener.region).send(new DeleteListenerCommand({ ListenerArn: listener.ListenerArn }));
			toast.success('Listener deleted');
			if (selectedListener?.ListenerArn === listener.ListenerArn) {
				selectedListener = null;
				listenerRules = [];
				listenerCerts = [];
			}
			await loadAllListeners();
		} catch (e) {
			toast.error(`Failed to delete: ${e}`);
		}
	}

	async function createTrustStore() {
		if (!newTSName.trim()) { toast.error('Name is required'); return; }
		creatingTS = true;
		try {
			await elb().send(new CreateTrustStoreCommand({
				Name: newTSName.trim(),
				CaCertificatesBundleS3Bucket: TRUST_STORE_CA_S3_BUCKET,
				CaCertificatesBundleS3Key: TRUST_STORE_CA_S3_KEY
			}));
			toast.success(`Trust store "${newTSName}" created`);
			showCreateTSModal = false;
			newTSName = '';
			await loadTrustStores();
		} catch (e) {
			toast.error(`Failed to create trust store: ${e}`);
		} finally {
			creatingTS = false;
		}
	}

	async function deleteTrustStore(ts: Regioned<TrustStore>) {
		if (!ts.TrustStoreArn) return;
		const confirmed = await confirmDestructive({
			title: 'Delete Trust Store',
			message: `Delete trust store "${ts.Name}"?`
		});
		if (!confirmed) return;
		try {
			await getELBv2Client(ts.region).send(new DeleteTrustStoreCommand({ TrustStoreArn: ts.TrustStoreArn }));
			toast.success(`Deleted "${ts.Name}"`);
			if (selectedTrustStore?.TrustStoreArn === ts.TrustStoreArn) selectedTrustStore = null;
			await loadTrustStores();
		} catch (e) {
			toast.error(`Failed to delete: ${e}`);
		}
	}

	async function seedDemoData() {
		try {
			toast.info('Seeding demo data…');
			const [albRes, nlbRes] = await Promise.all([
				elb().send(new CreateLoadBalancerCommand({ Name: 'demo-alb', Type: 'application', Scheme: 'internet-facing', Subnets: ['subnet-demo1', 'subnet-demo2'] })),
				elb().send(new CreateLoadBalancerCommand({ Name: 'demo-nlb', Type: 'network', Scheme: 'internal', Subnets: ['subnet-demo1'] }))
			]);
			const albArn = albRes.LoadBalancers?.[0]?.LoadBalancerArn ?? '';
			const [tg1Res, tg2Res] = await Promise.all([
				elb().send(new CreateTargetGroupCommand({ Name: 'demo-tg-web', TargetType: 'instance', Protocol: 'HTTP', Port: 80 })),
				elb().send(new CreateTargetGroupCommand({ Name: 'demo-tg-api', TargetType: 'ip', Protocol: 'HTTP', Port: 8080 }))
			]);
			const tg1Arn = tg1Res.TargetGroups?.[0]?.TargetGroupArn ?? '';
			const listenerRes = await elb().send(new CreateListenerCommand({
				LoadBalancerArn: albArn,
				Protocol: 'HTTP',
				Port: 80,
				DefaultActions: [{ Type: 'forward', TargetGroupArn: tg1Arn }]
			}));
			const listenerArn = listenerRes.Listeners?.[0]?.ListenerArn ?? '';
			if (listenerArn) {
				await Promise.all([
					elb().send(new CreateRuleCommand({ ListenerArn: listenerArn, Priority: 10, Conditions: [{ Field: 'path-pattern', PathPatternConfig: { Values: ['/api/*'] } }], Actions: [{ Type: 'forward', TargetGroupArn: tg2Res.TargetGroups?.[0]?.TargetGroupArn }] })),
					elb().send(new CreateRuleCommand({ ListenerArn: listenerArn, Priority: 20, Conditions: [{ Field: 'host-header', HostHeaderConfig: { Values: ['app.example.com'] } }], Actions: [{ Type: 'forward', TargetGroupArn: tg1Arn }] }))
				]);
			}
			await elb().send(new CreateTrustStoreCommand({ Name: 'demo-trust-store', CaCertificatesBundleS3Bucket: TRUST_STORE_CA_S3_BUCKET, CaCertificatesBundleS3Key: TRUST_STORE_CA_S3_KEY }));
			toast.success('Demo data seeded successfully');
			await Promise.all([loadLoadBalancers(), loadTargetGroups(), loadAllListeners(), loadTrustStores()]);
		} catch (e) {
			toast.error(`Seed failed: ${e}`);
		}
	}

	async function onTabChange(tab: Tab) {
		activeTab = tab;
		searchQuery = '';
		selectedLB = null;
		selectedTG = null;
		selectedListener = null;
		selectedTrustStore = null;
		if (tab === 'lbs') await loadLoadBalancers();
		else if (tab === 'targets') await loadTargetGroups();
		else if (tab === 'listeners') await loadAllListeners();
		else if (tab === 'truststores') await loadTrustStores();
		else if (tab === 'metrics') await Promise.all([loadLoadBalancers(), loadTargetGroups(), loadAllListeners(), loadTrustStores()]);
	}

	// Every list, the four "selected*" master-detail pointers, and their
	// detail sub-lists are all region-scoped: an ARN from the old region is
	// meaningless in the new one.
	onRegionChange(() => {
		selectedLB = null;
		lbListeners = [];
		selectedTG = null;
		tgHealth = [];
		selectedListener = null;
		listenerRules = [];
		listenerCerts = [];
		selectedTrustStore = null;
		trustStoreRevocations = [];
		loadBalancers = [];
		targetGroups = [];
		allListeners = [];
		trustStores = [];
		void Promise.all([loadLoadBalancers(), loadTargetGroups(), loadAllListeners(), loadTrustStores()]);
	});
</script>

<div class="space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Network class="h-8 w-8 text-teal-600" />
			<div>
				<h1 class="text-2xl font-bold">Elastic Load Balancing v2</h1>
				<p class="text-sm text-muted-foreground">
					Application, Network, and Gateway Load Balancers
				</p>
			</div>
		</div>
		<div class="flex items-center gap-2">
			<button
				onclick={seedDemoData}
				class="flex items-center gap-2 rounded-md border px-3 py-2 text-sm hover:bg-accent"
			>
				<Package class="h-4 w-4" />
				Demo
			</button>
			<button
				onclick={() => {
					if (activeTab === 'lbs') loadLoadBalancers();
					else if (activeTab === 'targets') loadTargetGroups();
					else if (activeTab === 'listeners') loadAllListeners();
					else if (activeTab === 'truststores') loadTrustStores();
					else if (activeTab === 'metrics') Promise.all([loadLoadBalancers(), loadTargetGroups(), loadAllListeners(), loadTrustStores()]);
				}}
				class="flex items-center gap-2 rounded-md border px-3 py-2 text-sm hover:bg-accent"
			>
				<RefreshCw class="h-4 w-4" />
				Refresh
			</button>
		</div>
	</div>

	<!-- Tabs -->
	<div class="flex overflow-x-auto border-b">
		{#each [
			{ id: 'lbs', label: 'Load Balancers', count: loadBalancers.length },
			{ id: 'targets', label: 'Target Groups', count: targetGroups.length },
			{ id: 'listeners', label: 'Listeners & Rules', count: allListeners.length },
			{ id: 'truststores', label: 'Trust Stores', count: trustStores.length },
			{ id: 'metrics', label: 'Metrics', count: null },
			{ id: 'docs', label: 'Docs', count: null }
		] as tab}
			<button
				onclick={() => onTabChange(tab.id as Tab)}
				class="flex shrink-0 items-center gap-1.5 px-4 py-2 text-sm font-medium border-b-2 transition-colors {activeTab === tab.id ? 'border-primary text-primary' : 'border-transparent text-muted-foreground hover:text-foreground'}"
			>
				{tab.label}
				{#if tab.count !== null && tab.count > 0}
					<span class="rounded-full bg-muted px-1.5 py-0.5 text-xs font-normal text-muted-foreground">{tab.count}</span>
				{/if}
			</button>
		{/each}
	</div>

	<!-- ── Load Balancers Tab ─────────────────────────────────────────────────── -->
	{#if activeTab === 'lbs'}
		<div class="flex items-center justify-between gap-4">
			<div class="relative flex-1">
				<Search class="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
				<input
					type="text"
					placeholder="Search load balancers..."
					bind:value={searchQuery}
					class="w-full rounded-md border bg-background pl-9 pr-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
				/>
			</div>
			<button
				onclick={() => (showCreateModal = true)}
				class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90"
			>
				<Plus class="h-4 w-4" />
				Create
			</button>
		</div>

		{#if loading}
			<div class="flex justify-center py-12">
				<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
			</div>
		{:else if filteredLBs.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<Network class="h-12 w-12 mb-3 opacity-30" />
				<p>No load balancers found</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Name</th>
							<th class="px-4 py-3 text-left font-medium">Type</th>
							<th class="px-4 py-3 text-left font-medium">Scheme</th>
							<th class="px-4 py-3 text-left font-medium">State</th>
							<th class="px-4 py-3 text-left font-medium">DNS Name</th>
							<th class="px-4 py-3 text-right font-medium">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each filteredLBs as lb (lb.region + '::' + lb.LoadBalancerArn)}
							<tr
								class="hover:bg-muted/30 cursor-pointer"
								onclick={() => viewLBListeners(lb)}
							>
								<td class="px-4 py-3 font-medium">
									<div class="flex items-center gap-2">
										{lb.LoadBalancerName}
										<RegionChip region={lb.region} />
									</div>
								</td>
								<td class="px-4 py-3">
									<span class="rounded-full px-2 py-0.5 text-xs font-medium capitalize {lbTypeBadge(lb.Type)}">
										{lb.Type ?? '—'}
									</span>
								</td>
								<td class="px-4 py-3 text-muted-foreground capitalize">{lb.Scheme ?? '—'}</td>
								<td class="px-4 py-3">
									<span class="rounded-full px-2 py-0.5 text-xs font-medium {lbStateBadge(lb.State?.Code)}">
										{lb.State?.Code ?? '—'}
									</span>
								</td>
								<td class="px-4 py-3 text-xs text-muted-foreground truncate max-w-[200px]">
									{lb.DNSName ?? '—'}
								</td>
								<td class="px-4 py-3 text-right">
									<div class="flex items-center justify-end gap-1">
										{#if lb.LoadBalancerArn}
											<button
												onclick={(e) => { e.stopPropagation(); copyToClipboard(lb.LoadBalancerArn!, 'ARN'); }}
												class="rounded p-1 text-muted-foreground hover:bg-muted"
												title="Copy ARN"
											>
												<Copy class="h-3.5 w-3.5" />
											</button>
										{/if}
										<button
											onclick={(e) => { e.stopPropagation(); deleteLB(lb); }}
											class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950"
											title="Delete"
										>
											<Trash2 class="h-4 w-4" />
										</button>
									</div>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}

		<!-- LB Listeners Drawer -->
		{#if selectedLB}
			<div class="rounded-lg border p-4 space-y-3">
				<div class="flex items-center justify-between">
					<h3 class="font-semibold flex items-center gap-2">{selectedLB.LoadBalancerName} — Listeners <RegionChip region={selectedLB.region} /></h3>
					<button onclick={() => (selectedLB = null)} class="text-xs text-muted-foreground hover:text-foreground">
						Close
					</button>
				</div>
				{#if loadingListeners}
					<RefreshCw class="h-5 w-5 animate-spin text-muted-foreground" />
				{:else if lbListeners.length === 0}
					<p class="text-sm text-muted-foreground">No listeners configured.</p>
				{:else}
					<div class="divide-y rounded border overflow-hidden">
						{#each lbListeners as listener}
							<div class="px-4 py-3 text-sm flex items-center justify-between">
								<div>
									<span class="font-medium">{listener.Protocol}:{listener.Port}</span>
									<span class="ml-2 text-xs text-muted-foreground">
										{listener.DefaultActions?.map((a) => a.Type).join(', ') ?? '—'}
									</span>
								</div>
								<ChevronRight class="h-4 w-4 text-muted-foreground" />
							</div>
						{/each}
					</div>
				{/if}
			</div>
		{/if}
	{/if}

	<!-- ── Target Groups Tab ──────────────────────────────────────────────────── -->
	{#if activeTab === 'targets'}
		<div class="flex items-center justify-between gap-4">
			<div class="relative flex-1">
				<Search class="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
				<input
					type="text"
					placeholder="Search target groups..."
					bind:value={searchQuery}
					class="w-full rounded-md border bg-background pl-9 pr-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
				/>
			</div>
			<button
				onclick={() => (showCreateTGModal = true)}
				class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90"
			>
				<Plus class="h-4 w-4" />
				Create
			</button>
		</div>

		{#if loading}
			<div class="flex justify-center py-12">
				<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
			</div>
		{:else if filteredTGs.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<Server class="h-12 w-12 mb-3 opacity-30" />
				<p>No target groups found</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Name</th>
							<th class="px-4 py-3 text-left font-medium">Protocol</th>
							<th class="px-4 py-3 text-left font-medium">Port</th>
							<th class="px-4 py-3 text-left font-medium">Target Type</th>
							<th class="px-4 py-3 text-left font-medium">Health Check</th>
							<th class="px-4 py-3 text-right font-medium">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each filteredTGs as tg (tg.region + '::' + tg.TargetGroupArn)}
							<tr
								class="hover:bg-muted/30 cursor-pointer"
								onclick={() => viewTGHealth(tg)}
							>
								<td class="px-4 py-3 font-medium">
									<div class="flex items-center gap-2">
										{tg.TargetGroupName}
										<RegionChip region={tg.region} />
									</div>
								</td>
								<td class="px-4 py-3 text-muted-foreground">{tg.Protocol ?? '—'}</td>
								<td class="px-4 py-3">{tg.Port ?? '—'}</td>
								<td class="px-4 py-3">
									<span class="rounded-full px-2 py-0.5 text-xs font-medium capitalize {tgTypeBadge(tg.TargetType)}">
										{tg.TargetType ?? '—'}
									</span>
								</td>
								<td class="px-4 py-3 text-muted-foreground">{tg.HealthCheckPath ?? '/'}</td>
								<td class="px-4 py-3 text-right">
									<div class="flex items-center justify-end gap-1">
										{#if tg.TargetGroupArn}
											<button
												onclick={(e) => { e.stopPropagation(); copyToClipboard(tg.TargetGroupArn!, 'ARN'); }}
												class="rounded p-1 text-muted-foreground hover:bg-muted"
												title="Copy ARN"
											>
												<Copy class="h-3.5 w-3.5" />
											</button>
										{/if}
										<button
											onclick={(e) => { e.stopPropagation(); deleteTG(tg); }}
											class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950"
											title="Delete"
										>
											<Trash2 class="h-4 w-4" />
										</button>
									</div>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>

			<!-- Target Health -->
			{#if selectedTG}
				<div class="rounded-lg border p-4 space-y-3">
					<div class="flex items-center justify-between">
						<h3 class="font-semibold flex items-center gap-2">{selectedTG.TargetGroupName} — Target Health <RegionChip region={selectedTG.region} /></h3>
						<button onclick={() => (selectedTG = null)} class="text-xs text-muted-foreground hover:text-foreground">
							Close
						</button>
					</div>
					{#if loadingHealth}
						<RefreshCw class="h-5 w-5 animate-spin text-muted-foreground" />
					{:else}
						{#if tgHealth.length === 0}
							<p class="text-sm text-muted-foreground">No registered targets.</p>
						{:else}
							<div class="divide-y rounded border overflow-hidden">
								{#each tgHealth as h}
									<div class="px-4 py-3 text-sm flex items-center justify-between">
										<span>{h.Target?.Id}:{h.Target?.Port}</span>
										<div class="flex items-center gap-2">
											<span class="rounded-full px-2 py-0.5 text-xs {healthBadge(h.TargetHealth?.State)}">
												{h.TargetHealth?.State ?? '—'}
											</span>
											<button onclick={() => deregisterTarget(h.Target?.Id, h.Target?.Port)} class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950" title="Deregister target">
												<Trash2 class="h-3.5 w-3.5" />
											</button>
										</div>
									</div>
								{/each}
							</div>
						{/if}

						<!-- Register IP/Instance target -->
						<div class="border-t pt-3 space-y-2">
							<h4 class="text-sm font-medium">Register Target {selectedTG.TargetType === 'ip' ? '(IP)' : selectedTG.TargetType === 'instance' ? '(Instance)' : ''}</h4>
							<div class="flex flex-wrap items-end gap-2">
								<div class="flex-1 min-w-[160px]">
									<label for="tg-target-id" class="block text-xs text-muted-foreground mb-1">{selectedTG.TargetType === 'ip' ? 'IP address' : selectedTG.TargetType === 'lambda' ? 'Lambda ARN' : 'Instance ID / IP'}</label>
									<input id="tg-target-id" type="text" bind:value={newTargetId} placeholder={selectedTG.TargetType === 'ip' ? '10.0.0.10' : 'i-0123… or 10.0.0.10'} class="w-full rounded-md border bg-background px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
								</div>
								<div class="w-24">
									<label for="tg-target-port" class="block text-xs text-muted-foreground mb-1">Port</label>
									<input id="tg-target-port" type="number" bind:value={newTargetPort} class="w-full rounded-md border bg-background px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
								</div>
								<button onclick={registerTarget} disabled={registeringTarget || !newTargetId.trim()} class="rounded-md bg-primary px-3 py-1.5 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50">
									{registeringTarget ? 'Registering…' : 'Register'}
								</button>
							</div>
						</div>

						<!-- Stickiness -->
						<div class="border-t pt-3 space-y-2">
							<h4 class="text-sm font-medium">Stickiness</h4>
							<label class="flex items-center gap-2 text-sm">
								<input type="checkbox" bind:checked={tgStickinessEnabled} class="rounded" />
								Enable load-balancer cookie stickiness
							</label>
							{#if tgStickinessEnabled}
								<div class="flex items-center gap-2">
									<label for="tg-stick-dur" class="text-xs text-muted-foreground">Duration (s)</label>
									<input id="tg-stick-dur" type="number" min="1" max="604800" bind:value={tgStickinessDuration} class="w-32 rounded-md border bg-background px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
								</div>
							{/if}
							<button onclick={saveTgStickiness} disabled={savingTgAttrs} class="rounded-md border px-3 py-1.5 text-sm hover:bg-muted disabled:opacity-50">
								{savingTgAttrs ? 'Saving…' : 'Save Stickiness'}
							</button>
						</div>
					{/if}
				</div>
			{/if}
		{/if}
	{/if}

	<!-- ── Listeners & Rules Tab ──────────────────────────────────────────────── -->
	{#if activeTab === 'listeners'}
		<div class="flex flex-wrap items-center gap-3">
			<div class="relative flex-1 min-w-[160px]">
				<Search class="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
				<input
					type="text"
					placeholder="Search by protocol or port..."
					bind:value={searchQuery}
					class="w-full rounded-md border bg-background pl-9 pr-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
				/>
			</div>
			<select
				bind:value={filterByLB}
				class="rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
			>
				<option value="">All load balancers</option>
				{#each loadBalancers as lb}
					<option value={lb.LoadBalancerArn}>{lb.LoadBalancerName} ({lb.region})</option>
				{/each}
			</select>
			<button
				onclick={() => (showCreateListenerModal = true)}
				class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90"
			>
				<Plus class="h-4 w-4" />
				Create
			</button>
		</div>

		{#if loadingAllListeners}
			<div class="flex justify-center py-12">
				<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
			</div>
		{:else if filteredListeners.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<List class="h-12 w-12 mb-3 opacity-30" />
				<p>No listeners found</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Protocol:Port</th>
							<th class="px-4 py-3 text-left font-medium">Default Action</th>
							<th class="px-4 py-3 text-left font-medium">SSL Policy</th>
							<th class="px-4 py-3 text-left font-medium">Certificates</th>
							<th class="px-4 py-3 text-right font-medium">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each filteredListeners as listener (listener.region + '::' + listener.ListenerArn)}
							<tr
								class="hover:bg-muted/30 cursor-pointer {selectedListener && selectedListener.ListenerArn === listener.ListenerArn && selectedListener.region === listener.region ? 'bg-muted/40' : ''}"
								onclick={() => selectListener(listener)}
							>
								<td class="px-4 py-3 font-medium">
									<span class="inline-flex items-center gap-1">
										{listener.Protocol}:{listener.Port}
										{#if listener.Protocol === 'HTTPS' || listener.Protocol === 'TLS'}
											<Lock class="h-3 w-3 text-green-500" />
										{/if}
										<RegionChip region={listener.region} />
									</span>
								</td>
								<td class="px-4 py-3 text-muted-foreground">
									{listener.DefaultActions?.map((a) => a.Type).join(', ') ?? '—'}
								</td>
								<td class="px-4 py-3 text-muted-foreground text-xs">
									{listener.SslPolicy ?? '—'}
								</td>
								<td class="px-4 py-3 text-muted-foreground">
									{listener.Certificates?.length ?? 0} cert(s)
								</td>
								<td class="px-4 py-3 text-right">
									<div class="flex items-center justify-end gap-1">
										{#if listener.ListenerArn}
											<button
												onclick={(e) => { e.stopPropagation(); copyToClipboard(listener.ListenerArn!, 'ARN'); }}
												class="rounded p-1 text-muted-foreground hover:bg-muted"
												title="Copy ARN"
											>
												<Copy class="h-3.5 w-3.5" />
											</button>
										{/if}
										<button
											onclick={(e) => { e.stopPropagation(); deleteListener(listener); }}
											class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950"
											title="Delete listener"
										>
											<Trash2 class="h-4 w-4" />
										</button>
									</div>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}

		<!-- Listener Detail Panel -->
		{#if selectedListener}
			<div class="rounded-lg border p-4 space-y-4">
				<div class="flex items-center justify-between">
					<h3 class="font-semibold flex items-center gap-2">{selectedListener.Protocol}:{selectedListener.Port} — Details <RegionChip region={selectedListener.region} /></h3>
					<button onclick={() => (selectedListener = null)} class="text-xs text-muted-foreground hover:text-foreground">
						Close
					</button>
				</div>

				<!-- ARN info -->
				<p class="text-xs text-muted-foreground break-all">ARN: {selectedListener.ListenerArn}</p>

				<!-- Rules section -->
				<div class="space-y-2">
					<div class="flex items-center justify-between">
						<h4 class="text-sm font-medium">Rules</h4>
						<button
							onclick={openCreateRule}
							class="flex items-center gap-1 text-xs text-primary hover:underline"
						>
							<Plus class="h-3 w-3" /> Add Rule
						</button>
					</div>
					{#if loadingRules}
						<RefreshCw class="h-4 w-4 animate-spin text-muted-foreground" />
					{:else if listenerRules.length === 0}
						<p class="text-sm text-muted-foreground">No rules. Click "Add Rule" to create one.</p>
					{:else}
						<div class="space-y-2">
							{#each [...listenerRules].sort((a, b) => {
								if (a.IsDefault) return 1;
								if (b.IsDefault) return -1;
								return (parseInt(a.Priority ?? '0', 10) || 0) - (parseInt(b.Priority ?? '0', 10) || 0);
							}) as rule}
								<div class="rounded border p-3 text-sm space-y-2">
									<div class="flex items-center justify-between">
										<div class="flex items-center gap-2">
											<span class="font-mono text-xs bg-muted px-1.5 py-0.5 rounded">
												{rule.IsDefault ? 'default' : `priority ${rule.Priority}`}
											</span>
											{#if rule.IsDefault}
												<span class="text-xs text-muted-foreground">Default rule</span>
											{/if}
										</div>
										{#if !rule.IsDefault}
											<div class="flex items-center gap-1">
												<button
													onclick={() => reorderRule(rule, -1)}
													class="rounded p-0.5 text-muted-foreground hover:bg-muted hover:text-foreground"
													title="Increase priority (move up)"
												>
													<ChevronDown class="h-3.5 w-3.5 rotate-180" />
												</button>
												<button
													onclick={() => reorderRule(rule, 1)}
													class="rounded p-0.5 text-muted-foreground hover:bg-muted hover:text-foreground"
													title="Decrease priority (move down)"
												>
													<ChevronDown class="h-3.5 w-3.5" />
												</button>
												<button
													onclick={() => openEditRule(rule)}
													class="text-xs text-blue-600 hover:underline dark:text-blue-400"
												>
													Edit
												</button>
												<button
													onclick={() => deleteRule(rule)}
													class="text-xs text-red-500 hover:underline"
												>
													Delete
												</button>
											</div>
										{/if}
									</div>

									<!-- Conditions visualization -->
									{#if (rule.Conditions?.length ?? 0) > 0}
										<div>
											<span class="text-xs text-muted-foreground mr-2">Conditions:</span>
											<div class="inline-flex flex-wrap gap-1 mt-1">
												{#each rule.Conditions ?? [] as cond}
													<span class="rounded-full px-2 py-0.5 text-xs font-medium {conditionBadgeColor(cond.Field)}">
														{conditionLabel(cond)}
													</span>
												{/each}
											</div>
										</div>
									{/if}

									<!-- Actions -->
									<div>
										<span class="text-xs text-muted-foreground mr-2">Actions:</span>
										{#each rule.Actions ?? [] as action}
											<span class="text-xs">
												{action.Type}
												{#if action.TargetGroupArn}
													→ <span class="font-mono text-xs">{action.TargetGroupArn.split('/').pop()}</span>
												{/if}
											</span>
										{/each}
									</div>
								</div>
							{/each}
						</div>
					{/if}
				</div>

				<!-- Certificates section -->
				<div class="space-y-2 border-t pt-3">
					<div class="flex items-center justify-between">
						<h4 class="text-sm font-medium flex items-center gap-1">
							<Shield class="h-4 w-4" /> Certificates
						</h4>
						<button
							onclick={() => (showAddCertModal = true)}
							class="flex items-center gap-1 text-xs text-primary hover:underline"
						>
							<Plus class="h-3 w-3" /> Add
						</button>
					</div>
					{#if loadingCerts}
						<RefreshCw class="h-4 w-4 animate-spin text-muted-foreground" />
					{:else if listenerCerts.length === 0}
						<p class="text-sm text-muted-foreground">No additional certificates.</p>
					{:else}
						<div class="space-y-1">
							{#each listenerCerts as cert}
								<div class="flex items-center justify-between text-sm rounded border px-3 py-2">
									<div class="flex items-center gap-2">
										<Lock class="h-3 w-3 text-green-500" />
										<span class="font-mono text-xs truncate max-w-[400px]">{cert.CertificateArn}</span>
										{#if cert.IsDefault}
											<span class="text-xs bg-green-100 text-green-700 dark:bg-green-900 dark:text-green-300 px-1.5 py-0.5 rounded-full">default</span>
										{/if}
									</div>
									{#if !cert.IsDefault}
										<button
											onclick={() => removeCert(cert.CertificateArn ?? '')}
											class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950"
										>
											<Trash2 class="h-3 w-3" />
										</button>
									{/if}
								</div>
							{/each}
						</div>
					{/if}
				</div>
			</div>
		{/if}
	{/if}

	<!-- ── Trust Stores Tab ───────────────────────────────────────────────────── -->
	{#if activeTab === 'truststores'}
		<div class="flex items-center justify-between gap-4">
			<div class="flex-1"></div>
			<button
				onclick={() => (showCreateTSModal = true)}
				class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90"
			>
				<Plus class="h-4 w-4" />
				Create
			</button>
		</div>
		{#if loadingTrustStores}
			<div class="flex justify-center py-12">
				<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
			</div>
		{:else if trustStores.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<Shield class="h-12 w-12 mb-3 opacity-30" />
				<p>No trust stores found</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Name</th>
							<th class="px-4 py-3 text-left font-medium">Status</th>
							<th class="px-4 py-3 text-left font-medium">Revoked Entries</th>
							<th class="px-4 py-3 text-right font-medium">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each trustStores as ts (ts.region + '::' + ts.TrustStoreArn)}
							<tr
								class="hover:bg-muted/30 cursor-pointer {selectedTrustStore && selectedTrustStore.TrustStoreArn === ts.TrustStoreArn && selectedTrustStore.region === ts.region ? 'bg-muted/40' : ''}"
								onclick={() => viewTrustStoreRevocations(ts)}
							>
								<td class="px-4 py-3 font-medium">
									<div class="flex items-center gap-2">
										{ts.Name ?? '—'}
										<RegionChip region={ts.region} />
									</div>
								</td>
								<td class="px-4 py-3">
									<span class="rounded-full px-2 py-0.5 text-xs font-medium {ts.Status === 'ACTIVE' ? 'text-green-700 bg-green-100 dark:text-green-300 dark:bg-green-900' : 'text-muted-foreground bg-muted'}">
										{ts.Status ?? '—'}
									</span>
								</td>
								<td class="px-4 py-3 text-muted-foreground">{ts.TotalRevokedEntries ?? 0}</td>
								<td class="px-4 py-3 text-right">
									<div class="flex items-center justify-end gap-1">
										{#if ts.TrustStoreArn}
											<button
												onclick={(e) => { e.stopPropagation(); copyToClipboard(ts.TrustStoreArn!, 'ARN'); }}
												class="rounded p-1 text-muted-foreground hover:bg-muted"
												title="Copy ARN"
											>
												<Copy class="h-3.5 w-3.5" />
											</button>
										{/if}
										<button
											onclick={(e) => { e.stopPropagation(); deleteTrustStore(ts); }}
											class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950"
											title="Delete"
										>
											<Trash2 class="h-4 w-4" />
										</button>
									</div>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>

			<!-- Trust Store Revocations -->
			{#if selectedTrustStore}
				<div class="rounded-lg border p-4 space-y-3">
					<div class="flex items-center justify-between">
						<h3 class="font-semibold flex items-center gap-2">{selectedTrustStore.Name} — Revocations <RegionChip region={selectedTrustStore.region} /></h3>
						<button onclick={() => (selectedTrustStore = null)} class="text-xs text-muted-foreground hover:text-foreground">
							Close
						</button>
					</div>
					{#if loadingRevocations}
						<RefreshCw class="h-5 w-5 animate-spin text-muted-foreground" />
					{:else if trustStoreRevocations.length === 0}
						<p class="text-sm text-muted-foreground">No revocation entries.</p>
					{:else}
						<div class="divide-y rounded border overflow-hidden">
							{#each trustStoreRevocations as rev}
								<div class="px-4 py-3 text-sm flex items-center justify-between">
									<div>
										<span class="font-mono text-xs">{rev.RevocationId ?? '—'}</span>
										{#if rev.RevocationType}
											<span class="ml-2 text-xs text-muted-foreground">{rev.RevocationType}</span>
										{/if}
									</div>
									{#if rev.NumberOfRevokedEntries !== undefined}
										<span class="text-xs text-muted-foreground">
											{rev.NumberOfRevokedEntries} revoked
										</span>
									{/if}
								</div>
							{/each}
						</div>
					{/if}
				</div>
			{/if}
		{/if}
	{/if}

	<!-- ── Metrics Tab ─────────────────────────────────────────────────────────── -->
	{#if activeTab === 'metrics'}
		<div class="grid grid-cols-2 gap-4 sm:grid-cols-4">
			<div class="rounded-lg border p-4 text-center space-y-1">
				<p class="text-3xl font-bold">{metrics.lbCount}</p>
				<p class="text-sm text-muted-foreground">Load Balancers</p>
				<div class="flex justify-center gap-1 flex-wrap pt-1">
					{#if metrics.albCount > 0}
						<span class="rounded-full px-2 py-0.5 text-xs {lbTypeBadge('application')}">ALB: {metrics.albCount}</span>
					{/if}
					{#if metrics.nlbCount > 0}
						<span class="rounded-full px-2 py-0.5 text-xs {lbTypeBadge('network')}">NLB: {metrics.nlbCount}</span>
					{/if}
					{#if metrics.gwbCount > 0}
						<span class="rounded-full px-2 py-0.5 text-xs {lbTypeBadge('gateway')}">GWB: {metrics.gwbCount}</span>
					{/if}
				</div>
			</div>
			<div class="rounded-lg border p-4 text-center space-y-1">
				<p class="text-3xl font-bold">{metrics.tgCount}</p>
				<p class="text-sm text-muted-foreground">Target Groups</p>
				<div class="flex justify-center gap-1 flex-wrap pt-1">
					{#if metrics.tgInstance > 0}
						<span class="rounded-full px-2 py-0.5 text-xs {tgTypeBadge('instance')}">instance: {metrics.tgInstance}</span>
					{/if}
					{#if metrics.tgIp > 0}
						<span class="rounded-full px-2 py-0.5 text-xs {tgTypeBadge('ip')}">ip: {metrics.tgIp}</span>
					{/if}
					{#if metrics.tgLambda > 0}
						<span class="rounded-full px-2 py-0.5 text-xs {tgTypeBadge('lambda')}">lambda: {metrics.tgLambda}</span>
					{/if}
				</div>
			</div>
			<div class="rounded-lg border p-4 text-center space-y-1">
				<p class="text-3xl font-bold">{metrics.listenerCount}</p>
				<p class="text-sm text-muted-foreground">Listeners</p>
			</div>
			<div class="rounded-lg border p-4 text-center space-y-1">
				<p class="text-3xl font-bold">{metrics.tsCount}</p>
				<p class="text-sm text-muted-foreground">Trust Stores</p>
			</div>
		</div>
	{/if}

	<!-- ── Docs Tab ────────────────────────────────────────────────────────────── -->
	{#if activeTab === 'docs'}
		<div class="space-y-4">
			<p class="text-sm text-muted-foreground">All supported ELBv2 operations available in this simulator.</p>
			{#each [
				{ category: 'Load Balancers', ops: ['CreateLoadBalancer', 'DescribeLoadBalancers', 'DeleteLoadBalancer', 'ModifyLoadBalancerAttributes', 'DescribeLoadBalancerAttributes', 'SetSecurityGroups', 'SetSubnets', 'SetIpAddressType'] },
				{ category: 'Target Groups', ops: ['CreateTargetGroup', 'DescribeTargetGroups', 'DeleteTargetGroup', 'ModifyTargetGroup', 'ModifyTargetGroupAttributes', 'DescribeTargetGroupAttributes', 'RegisterTargets', 'DeregisterTargets', 'DescribeTargetHealth'] },
				{ category: 'Listeners', ops: ['CreateListener', 'DescribeListeners', 'DeleteListener', 'ModifyListener', 'ModifyListenerAttributes', 'DescribeListenerAttributes'] },
				{ category: 'Rules', ops: ['CreateRule', 'DescribeRules', 'DeleteRule', 'ModifyRule', 'SetRulePriorities'] },
				{ category: 'Certificates', ops: ['AddListenerCertificates', 'DescribeListenerCertificates', 'RemoveListenerCertificates'] },
				{ category: 'Trust Stores', ops: ['CreateTrustStore', 'DescribeTrustStores', 'DeleteTrustStore', 'ModifyTrustStore', 'AddTrustStoreRevocations', 'DescribeTrustStoreRevocations', 'RemoveTrustStoreRevocations', 'DescribeTrustStoreAssociations'] },
				{ category: 'Tags', ops: ['AddTags', 'RemoveTags', 'DescribeTags'] },
				{ category: 'Misc', ops: ['DescribeAccountLimits', 'DescribeSSLPolicies', 'DescribeCapacityReservation'] }
			] as section}
				<div class="rounded-lg border overflow-hidden">
					<div class="bg-muted/50 px-4 py-2 font-medium text-sm">{section.category}</div>
					<div class="flex flex-wrap gap-2 p-3">
						{#each section.ops as op}
							<span class="rounded bg-muted px-2 py-1 text-xs font-mono">{op}</span>
						{/each}
					</div>
				</div>
			{/each}
		</div>
	{/if}
</div>
<!-- ── Create LB Modal ──────────────────────────────────────────────────────── -->
{#if showCreateModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg bg-background p-6 shadow-xl">
			<div class="flex items-center justify-between mb-4">
				<h2 class="text-lg font-semibold">Create Load Balancer</h2>
				<WriteRegionHint />
			</div>
			<div class="space-y-3">
				<div>
					<label for="lb-name" class="block text-sm font-medium mb-1">Name *</label>
					<input
						id="lb-name"
						type="text"
						bind:value={newLBName}
						placeholder="my-load-balancer"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<div>
					<label for="lb-type" class="block text-sm font-medium mb-1">Type</label>
					<select
						id="lb-type"
						bind:value={newLBType}
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					>
						<option value="application">Application</option>
						<option value="network">Network</option>
						<option value="gateway">Gateway</option>
					</select>
				</div>
				<div>
					<label for="lb-scheme" class="block text-sm font-medium mb-1">Scheme</label>
					<select
						id="lb-scheme"
						bind:value={newLBScheme}
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					>
						<option value="internet-facing">Internet-facing</option>
						<option value="internal">Internal</option>
					</select>
				</div>
				<div>
					<label for="lb-subnets" class="block text-sm font-medium mb-1"
						>Subnets (comma-separated) *</label
					>
					<input
						id="lb-subnets"
						type="text"
						bind:value={newLBSubnets}
						placeholder="subnet-abc123, subnet-def456"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
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
					onclick={createLB}
					disabled={creating || !newLBName.trim() || !newLBSubnets.trim()}
					class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					{creating ? 'Creating...' : 'Create'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- ── Add Certificate Modal ─────────────────────────────────────────────────── -->
{#if showAddCertModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg bg-background p-6 shadow-xl">
			<h2 class="text-lg font-semibold mb-4">Add Certificate</h2>
			<div class="space-y-3">
				<div>
					<label for="cert-arn" class="block text-sm font-medium mb-1">Certificate ARN *</label>
					<input
						id="cert-arn"
						type="text"
						bind:value={newCertArn}
						placeholder="arn:aws:acm:us-east-1:123456789012:certificate/..."
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button
					onclick={() => (showAddCertModal = false)}
					class="rounded-md border px-4 py-2 text-sm hover:bg-accent"
				>
					Cancel
				</button>
				<button
					onclick={addCert}
					disabled={addingCert || !newCertArn.trim()}
					class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					{addingCert ? 'Adding...' : 'Add'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- ── Rule Condition Editor Modal ────────────────────────────────────────────── -->
{#if showRuleEditor}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-lg rounded-lg bg-background p-6 shadow-xl">
			<h2 class="text-lg font-semibold mb-4">
				{editingRule ? 'Edit Rule' : 'Create Rule'}
			</h2>
			<div class="space-y-4">
				<!-- Priority (create only) -->
				{#if !editingRule}
					<div>
						<label for="rule-priority" class="block text-sm font-medium mb-1">Priority (1–50000)</label>
						<input
							id="rule-priority"
							type="number"
							min="1"
							max="50000"
							bind:value={rulePriority}
							placeholder="Auto-assign"
							class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
						/>
					</div>
				{/if}

				<!-- Condition type -->
				<div>
					<label for="cond-field" class="block text-sm font-medium mb-1">Condition Type</label>
					<select
						id="cond-field"
						bind:value={ruleCondField}
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					>
						<option value="host-header">Host Header</option>
						<option value="path-pattern">Path Pattern</option>
						<option value="http-header">HTTP Header</option>
						<option value="http-request-method">HTTP Method</option>
						<option value="source-ip">Source IP</option>
						<option value="query-string">Query String</option>
					</select>
				</div>

				{#if ruleCondField === 'http-header'}
					<div>
						<label for="header-name" class="block text-sm font-medium mb-1">Header Name *</label>
						<input
							id="header-name"
							type="text"
							bind:value={ruleCondHeaderName}
							placeholder="X-Custom-Header"
							class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
						/>
					</div>
				{/if}

				<div>
					<label for="cond-values" class="block text-sm font-medium mb-1">
						{#if ruleCondField === 'host-header'}Host names (comma-separated){/if}
						{#if ruleCondField === 'path-pattern'}Path patterns (comma-separated){/if}
						{#if ruleCondField === 'http-header'}Header values (comma-separated){/if}
						{#if ruleCondField === 'http-request-method'}Methods e.g. GET,POST{/if}
						{#if ruleCondField === 'source-ip'}CIDR blocks (comma-separated){/if}
						{#if ruleCondField === 'query-string'}Key=value pairs (comma-separated){/if}
					</label>
					<input
						id="cond-values"
						type="text"
						bind:value={ruleCondValues}
						placeholder={ruleCondField === 'host-header'
							? 'example.com, *.example.com'
							: ruleCondField === 'path-pattern'
								? '/api/*, /health'
								: ruleCondField === 'http-request-method'
									? 'GET, POST'
									: ruleCondField === 'source-ip'
										? '10.0.0.0/8, 192.168.0.0/16'
										: 'value1, value2'}
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>

				<!-- Action: forward to target group -->
				<div>
					<label for="action-tg" class="block text-sm font-medium mb-1">Forward to Target Group ARN</label>
					<input
						id="action-tg"
						type="text"
						bind:value={ruleActionTGArn}
						placeholder="arn:aws:elasticloadbalancing:..."
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
					{#if targetGroups.length > 0}
						<div class="mt-1 flex flex-wrap gap-1">
							{#each targetGroups.filter((tg) => !selectedListener || tg.region === selectedListener.region) as tg}
								<button
									onclick={() => (ruleActionTGArn = tg.TargetGroupArn ?? '')}
									class="text-xs rounded border px-2 py-0.5 hover:bg-accent"
								>
									{tg.TargetGroupName}
								</button>
							{/each}
						</div>
					{/if}
				</div>
			</div>

			<div class="mt-4 flex justify-end gap-2">
				<button
					onclick={() => (showRuleEditor = false)}
					class="rounded-md border px-4 py-2 text-sm hover:bg-accent"
				>
					Cancel
				</button>
				<button
					onclick={saveRule}
					disabled={savingRule || !ruleCondValues.trim()}
					class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					{savingRule ? 'Saving...' : editingRule ? 'Update' : 'Create'}
				</button>
			</div>
		</div>
	</div>
{/if}


<!-- ── Create Target Group Modal ─────────────────────────────────────────────── -->
{#if showCreateTGModal}
<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
<div class="w-full max-w-md rounded-lg bg-background p-6 shadow-xl">
<div class="flex items-center justify-between mb-4">
<h2 class="text-lg font-semibold">Create Target Group</h2>
<WriteRegionHint />
</div>
<div class="space-y-3">
<div>
<label for="tg-name" class="block text-sm font-medium mb-1">Name *</label>
<input id="tg-name" type="text" bind:value={newTGName} placeholder="my-target-group" class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
</div>
<div>
<label for="tg-type" class="block text-sm font-medium mb-1">Target Type</label>
<select id="tg-type" bind:value={newTGType} class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary">
<option value="instance">Instance</option>
<option value="ip">IP</option>
<option value="lambda">Lambda</option>
</select>
</div>
<div class="grid grid-cols-2 gap-3">
<div>
<label for="tg-protocol" class="block text-sm font-medium mb-1">Protocol</label>
<select id="tg-protocol" bind:value={newTGProtocol} class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary">
<option>HTTP</option>
<option>HTTPS</option>
<option>TCP</option>
<option>TLS</option>
<option>UDP</option>
</select>
</div>
<div>
<label for="tg-port" class="block text-sm font-medium mb-1">Port</label>
<input id="tg-port" type="number" bind:value={newTGPort} placeholder="80" class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
</div>
</div>
</div>
<div class="mt-4 flex justify-end gap-2">
<button onclick={() => (showCreateTGModal = false)} class="rounded-md border px-4 py-2 text-sm hover:bg-accent">Cancel</button>
<button onclick={createTG} disabled={creatingTG || !newTGName.trim()} class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50">
{creatingTG ? 'Creating...' : 'Create'}
</button>
</div>
</div>
</div>
{/if}

<!-- ── Create Listener Modal ─────────────────────────────────────────────────── -->
{#if showCreateListenerModal}
<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
<div class="w-full max-w-md rounded-lg bg-background p-6 shadow-xl">
<h2 class="text-lg font-semibold mb-4">Create Listener</h2>
<div class="space-y-3">
<div>
<label for="listener-lb" class="block text-sm font-medium mb-1">Load Balancer *</label>
<select id="listener-lb" bind:value={newListenerLBArn} class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary">
<option value="">Select load balancer…</option>
{#each loadBalancers as lb}
<option value={lb.LoadBalancerArn}>{lb.LoadBalancerName} ({lb.region})</option>
{/each}
</select>
<p class="text-xs text-muted-foreground mt-1">Created in the selected load balancer's region.</p>
</div>
<div class="grid grid-cols-2 gap-3">
<div>
<label for="listener-protocol" class="block text-sm font-medium mb-1">Protocol</label>
<select id="listener-protocol" bind:value={newListenerProtocol} class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary">
<option>HTTP</option>
<option>HTTPS</option>
<option>TCP</option>
<option>TLS</option>
<option>UDP</option>
</select>
</div>
<div>
<label for="listener-port" class="block text-sm font-medium mb-1">Port</label>
<input id="listener-port" type="number" bind:value={newListenerPort} placeholder="80" class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
</div>
</div>
{#if newListenerProtocol === 'HTTPS' || newListenerProtocol === 'TLS'}
<div>
<label for="listener-ssl" class="block text-sm font-medium mb-1">SSL Policy</label>
<input id="listener-ssl" type="text" bind:value={newListenerSslPolicy} placeholder="ELBSecurityPolicy-TLS13-1-2-2021-06" class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
</div>
{/if}
</div>
<div class="mt-4 flex justify-end gap-2">
<button onclick={() => (showCreateListenerModal = false)} class="rounded-md border px-4 py-2 text-sm hover:bg-accent">Cancel</button>
<button onclick={createListener} disabled={creatingListener || !newListenerLBArn} class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50">
{creatingListener ? 'Creating...' : 'Create'}
</button>
</div>
</div>
</div>
{/if}

<!-- ── Create Trust Store Modal ───────────────────────────────────────────────── -->
{#if showCreateTSModal}
<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
<div class="w-full max-w-md rounded-lg bg-background p-6 shadow-xl">
<div class="flex items-center justify-between mb-4">
<h2 class="text-lg font-semibold">Create Trust Store</h2>
<WriteRegionHint />
</div>
<div class="space-y-3">
<div>
<label for="ts-name" class="block text-sm font-medium mb-1">Name *</label>
<input id="ts-name" type="text" bind:value={newTSName} placeholder="my-trust-store" class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
</div>
<p class="text-xs text-muted-foreground">
The CA bundle will be sourced from the configured S3 bucket.
</p>
</div>
<div class="mt-4 flex justify-end gap-2">
<button onclick={() => (showCreateTSModal = false)} class="rounded-md border px-4 py-2 text-sm hover:bg-accent">Cancel</button>
<button onclick={createTrustStore} disabled={creatingTS || !newTSName.trim()} class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50">
{creatingTS ? 'Creating...' : 'Create'}
</button>
</div>
</div>
</div>
{/if}
