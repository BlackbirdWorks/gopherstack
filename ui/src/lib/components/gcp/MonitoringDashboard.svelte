<script lang="ts">
	import { onMount } from 'svelte';
	import { Activity } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import GcpScopeBar from '#lib/components/gcp/GcpScopeBar.svelte';
	import GcpResourceTable, { type ResourceRow } from '#lib/components/gcp/GcpResourceTable.svelte';
	import MetricSeries from '#lib/components/gcp/MetricSeries.svelte';
	import { confirmedAttempt, GCP_INPUT } from '#lib/gcp-action.ts';
	import {
		deleteResource,
		listAlertPolicies,
		listMetricDescriptors,
		listSnoozes,
		listUptimeChecks,
		readStoredMonitoringProject,
		snoozeState,
		writeStoredMonitoringProject,
		type AlertPolicy,
		type MetricDescriptor,
		type Snooze,
		type UptimeCheck
	} from '#lib/gcpmonitoring.ts';
	import { describeGcpError } from '#lib/gcprest.ts';

	const TABS = [
		{ id: 'metrics', label: 'Metrics' },
		{ id: 'policies', label: 'Alert policies' },
		{ id: 'uptime', label: 'Uptime checks' },
		{ id: 'snoozes', label: 'Snoozes' }
	];

	let project = $state(readStoredMonitoringProject());
	let tab = $state('metrics');
	let descriptors = $state<MetricDescriptor[]>([]);
	let policies = $state<AlertPolicy[]>([]);
	let checks = $state<UptimeCheck[]>([]);
	let snoozes = $state<Snooze[]>([]);
	let filter = $state('');
	let metric = $state('');
	let loading = $state(true);
	let error = $state('');

	async function load() {
		try {
			[descriptors, policies, checks, snoozes] = await Promise.all([listMetricDescriptors(project), listAlertPolicies(project), listUptimeChecks(project), listSnoozes(project)]);
			descriptors = descriptors.toSorted((a, b) => a.type.localeCompare(b.type));
			error = '';
			if (!descriptors.some((d) => d.type === metric)) metric = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	function applyScope(p: string) {
		project = p;
		writeStoredMonitoringProject(p);
		metric = '';
		loading = true;
		void load();
	}

	async function remove(kind: string, row: ResourceRow) {
		const ok = await confirmedAttempt({ title: `Delete ${kind}`, message: `Delete ${kind} "${row.cells[0]}"?` }, () => deleteResource(row.id), `Deleted ${kind} ${row.cells[0]}`, `Failed to delete ${row.cells[0]}`);
		if (ok) await load();
	}

	const shown = $derived(descriptors.filter((d) => d.type.toLowerCase().includes(filter.trim().toLowerCase())));
	const metricRows = $derived(shown.map((d) => ({ id: d.type, cells: [d.type, d.metricKind, d.valueType, d.unit, d.displayName], raw: d.raw })));
	const policyRows = $derived(policies.map((p) => ({ id: p.name, cells: [p.displayName || p.id, p.enabled ? 'enabled' : 'disabled', p.conditions.join(', '), `${p.channels}`], raw: p.raw })));
	const checkRows = $derived(checks.map((c) => ({ id: c.name, cells: [c.displayName || c.id, c.protocol, c.target, `${c.periodSeconds}s`], raw: c.raw })));
	const snoozeRows = $derived(snoozes.map((s) => ({ id: s.name, cells: [s.displayName || s.id, snoozeState(s), s.start, s.end], raw: s.raw })));

	onMount(load);
</script>

<div class="space-y-6 pb-20" data-testid="monitoring-dashboard">
	<PageHeader icon={Activity} title="Cloud Monitoring" description="Google Cloud Monitoring: metrics, time series, alert policies, uptime checks and snoozes" color="blue" onRefresh={load} />
	<GcpScopeBar prefix="monitoring" {project} hint="Resources are listed per project." onApply={applyScope} />
	<Tabs tabs={TABS} active={tab} onSelect={(id) => (tab = id)} color="blue" />

	{#if tab === 'metrics'}
		<input class="{GCP_INPUT} max-w-sm" placeholder="Filter metric types" bind:value={filter} aria-label="Filter metric types" data-testid="monitoring-filter" />
		<GcpResourceTable testId="monitoring-metric" noun="metric descriptors" columns={['Metric type', 'Kind', 'Value type', 'Unit', 'Display name']} rows={metricRows} {loading} {error} selected={metric} onRetry={load} onSelect={(r) => (metric = r.id)} />
		{#if metric}
			<MetricSeries {project} metricType={metric} />
		{/if}
	{:else if tab === 'policies'}
		<GcpResourceTable testId="monitoring-policy" noun="alert policies" columns={['Policy', 'State', 'Conditions', 'Channels']} rows={policyRows} {loading} {error} onRetry={load} onDelete={(r) => remove('alert policy', r)} />
	{:else if tab === 'uptime'}
		<GcpResourceTable testId="monitoring-uptime" noun="uptime checks" columns={['Check', 'Protocol', 'Target', 'Period']} rows={checkRows} {loading} {error} onRetry={load} onDelete={(r) => remove('uptime check', r)} />
	{:else}
		<GcpResourceTable testId="monitoring-snooze" noun="snoozes" columns={['Snooze', 'State', 'Start', 'End']} rows={snoozeRows} {loading} {error} onRetry={load} />
	{/if}
</div>
