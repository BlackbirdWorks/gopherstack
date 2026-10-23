<script lang="ts">
	import { onMount } from 'svelte';
	import { toast } from 'svelte-sonner';
	import { Database, Inbox } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import { attempt, GCP_INPUT, GCP_GHOST } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import { readStoredProject, validateProjectId, writeStoredProject } from '#lib/gcpproject.ts';
	import {
		SQL_PROJECT_KEY,
		listDatabases,
		listInstances,
		listUsers,
		stateTone,
		type SqlDatabase,
		type SqlInstance,
		type SqlUser
	} from '#lib/gcpsql.ts';

	let project = $state(readStoredProject(SQL_PROJECT_KEY));
	let projectInput = $state(readStoredProject(SQL_PROJECT_KEY));
	let instances = $state<SqlInstance[]>([]);
	let current = $state<SqlInstance | null>(null);
	let databases = $state<SqlDatabase[]>([]);
	let users = $state<SqlUser[]>([]);
	let loading = $state(true);
	let loadError = $state('');

	const projectError = $derived(validateProjectId(projectInput.trim()));

	async function load() {
		loading = true;
		loadError = '';
		try {
			instances = await listInstances(project);
			const keep = instances.find((i) => i.name === current?.name) ?? null;
			current = keep ?? null;
			if (keep) await loadDetail(keep);
		} catch (e) {
			loadError = describeGcpError(e);
			toast.error(`Failed to load Cloud SQL: ${loadError}`);
		} finally {
			loading = false;
		}
	}

	async function loadDetail(inst: SqlInstance) {
		[databases, users] = await Promise.all([listDatabases(project, inst.name), listUsers(project, inst.name)]);
	}

	async function select(inst: SqlInstance) {
		current = inst;
		databases = [];
		users = [];
		await attempt(() => loadDetail(inst), '', 'Failed to load instance details');
	}

	async function applyProject(e: Event) {
		e.preventDefault();
		if (projectError) return;
		project = projectInput.trim();
		writeStoredProject(SQL_PROJECT_KEY, project);
		current = null;
		await load();
	}

	onMount(() => void load());
</script>

<div class="space-y-6 pb-20" data-testid="sql-dashboard">
	<PageHeader icon={Database} title="Cloud SQL" description="Google Cloud SQL: instances, databases and users" color="blue" onRefresh={load} />

	<form class="flex flex-wrap items-end gap-2" onsubmit={applyProject}>
		<label class="text-sm">Project<input class={GCP_INPUT} bind:value={projectInput} data-testid="sql-project" /></label>
		<button type="submit" class={GCP_GHOST} disabled={Boolean(projectError)}>Apply</button>
		{#if projectError}<span class="text-sm text-red-600">{projectError}</span>{/if}
	</form>

	{#if loadError}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="sql-error">
			<p class="font-medium">Could not reach Cloud SQL</p>
			<p>{loadError}</p>
		</div>
	{:else if loading}
		<p class="text-gray-500">Loading...</p>
	{:else if instances.length === 0}
		<div class="flex flex-col items-center py-16 text-gray-500 dark:text-gray-400" data-testid="sql-empty">
			<Inbox class="h-10 w-10 mb-3 opacity-30" />
			<p>No instances in project {project}. Create one with gcloud, Terraform or the Cloud SQL Admin API.</p>
		</div>
	{:else}
		<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
			<table class="w-full text-sm" data-testid="sql-instances">
				<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class="px-4 py-2">Instance</th><th class="px-4 py-2">State</th><th class="px-4 py-2">Version</th><th class="px-4 py-2">Tier</th><th class="px-4 py-2">Region</th><th class="px-4 py-2">Address</th></tr></thead>
				<tbody>
					{#each instances as i (i.name)}
						<tr class="border-t border-gray-100 dark:border-gray-800 {i.name === current?.name ? 'bg-blue-50 dark:bg-slate-800' : ''}" data-testid="sql-instance-row">
							<td class="px-4 py-2"><button class="font-medium text-blue-600 underline dark:text-blue-400" onclick={() => select(i)}>{i.name}</button></td>
							<td class="px-4 py-2"><span class="rounded px-2 py-0.5 text-xs {stateTone(i.state)}">{i.state}</span></td>
							<td class="px-4 py-2">{i.databaseVersion}</td>
							<td class="px-4 py-2">{i.tier || '-'}</td>
							<td class="px-4 py-2">{i.region}</td>
							<td class="px-4 py-2 font-mono">{i.ipAddresses.join(', ') || '-'}</td>
						</tr>
					{/each}
				</tbody>
			</table>
		</div>

		{#if current}
			<p class="text-sm text-gray-500 dark:text-gray-400">Connection name: <span class="font-mono">{current.connectionName}</span></p>
			<div class="grid gap-6 md:grid-cols-2">
				<section data-testid="sql-databases">
					<h2 class="mb-2 font-semibold">Databases</h2>
					<table class="w-full text-sm"><thead class="text-left text-gray-500"><tr><th class="px-4 py-2">Name</th><th class="px-4 py-2">Charset</th><th class="px-4 py-2">Collation</th></tr></thead>
						<tbody>{#each databases as d (d.name)}<tr class="border-t border-gray-100 dark:border-gray-800"><td class="px-4 py-2">{d.name}</td><td class="px-4 py-2">{d.charset || '-'}</td><td class="px-4 py-2">{d.collation || '-'}</td></tr>{:else}<tr><td class="px-4 py-3 text-gray-500" colspan="3">No databases.</td></tr>{/each}</tbody></table>
				</section>
				<section data-testid="sql-users">
					<h2 class="mb-2 font-semibold">Users</h2>
					<table class="w-full text-sm"><thead class="text-left text-gray-500"><tr><th class="px-4 py-2">Name</th><th class="px-4 py-2">Host</th><th class="px-4 py-2">Type</th></tr></thead>
						<tbody>{#each users as u (u.name + u.host)}<tr class="border-t border-gray-100 dark:border-gray-800"><td class="px-4 py-2">{u.name}</td><td class="px-4 py-2">{u.host || '-'}</td><td class="px-4 py-2">{u.type || 'BUILT_IN'}</td></tr>{:else}<tr><td class="px-4 py-3 text-gray-500" colspan="3">No users.</td></tr>{/each}</tbody></table>
				</section>
			</div>
		{:else}
			<p class="text-sm text-gray-500">Select an instance to see its databases and users.</p>
		{/if}
	{/if}
</div>
