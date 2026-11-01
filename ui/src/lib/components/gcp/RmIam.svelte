<script lang="ts">
	import { RefreshCw } from 'lucide-svelte';
	import { toast } from 'svelte-sonner';
	import { GCP_INPUT, GCP_MINI, GCP_PRIMARY } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import { addMember, getIamPolicy, groupBindings, removeMember, validMember, validRole, type IamPolicy, type RmNode } from '#lib/gcpresourcemanager.ts';

	let { node }: { node: RmNode } = $props();

	let policy = $state<IamPolicy>({ etag: '', bindings: [] });
	let loading = $state(true);
	let error = $state('');
	let role = $state('roles/viewer');
	let member = $state('');
	let conflict = $state('');

	const bindings = $derived(groupBindings(policy));
	const formOk = $derived(validRole(role.trim()) && validMember(member.trim()));

	async function load() {
		try {
			policy = await getIamPolicy(node);
			error = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	$effect(() => {
		void node.name;
		loading = true;
		conflict = '';
		void load();
	});

	async function write(action: () => Promise<IamPolicy>, ok: string) {
		conflict = '';
		try {
			policy = await action();
			error = '';
			toast.success(ok);
		} catch (e) {
			conflict = describeGcpError(e);
			await load();
		}
	}

	async function add() {
		if (!formOk) return;
		await write(() => addMember(node, role.trim(), member.trim(), policy.etag), `Added ${member.trim()} to ${role.trim()}`);
		member = '';
	}

	async function remove(r: string, m: string) {
		await write(() => removeMember(node, r, m, policy.etag), `Removed ${m} from ${r}`);
	}
</script>

<div class="space-y-3" data-testid="rm-iam">
	{#if error}
		<p class="text-sm text-red-600" role="alert" data-testid="rm-iam-error">{error}</p>
	{:else if loading}
		<div class="flex justify-center py-6"><RefreshCw class="h-5 w-5 animate-spin text-gray-400" /></div>
	{:else}
		{#if conflict}<p class="rounded border border-red-200 bg-red-50 p-2 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="rm-iam-conflict">{conflict} The policy was reloaded.</p>{/if}
		{#if bindings.length === 0}
			<p class="py-3 text-sm text-gray-500" data-testid="rm-iam-empty">No bindings on this {node.kind}.</p>
		{:else}
			<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
				<table class="w-full text-sm" data-testid="rm-iam-table">
					<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400"><tr><th class="px-3 py-2">Role</th><th class="px-3 py-2">Members</th></tr></thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each bindings as b, i (`${b.role}/${b.condition}/${i}`)}
							<tr data-testid="rm-iam-binding" data-role={b.role}>
								<td class="px-3 py-2 align-top font-mono text-xs">{b.role}{#if b.condition}<div class="text-amber-600">condition: {b.condition}</div>{/if}</td>
								<td class="px-3 py-2">
									{#each b.members as m (m)}
										<div class="flex items-center gap-2 py-0.5 text-xs" data-testid="rm-iam-member">
											<span class="font-mono">{m}</span>
											{#if !b.condition}<button class={GCP_MINI} onclick={() => remove(b.role, m)} aria-label="Remove {m} from {b.role}" data-testid="rm-iam-remove">Remove</button>{/if}
										</div>
									{/each}
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
		<form class="flex flex-wrap items-end gap-2 text-sm" onsubmit={(e) => { e.preventDefault(); void add(); }} data-testid="rm-iam-form">
			<label class="block">Role<input class="{GCP_INPUT} !w-64 font-mono" bind:value={role} data-testid="rm-iam-role" /></label>
			<label class="block">Member<input class="{GCP_INPUT} !w-72 font-mono" placeholder="user:alice@example.com" bind:value={member} data-testid="rm-iam-member-input" /></label>
			<button type="submit" class={GCP_PRIMARY} disabled={!formOk} data-testid="rm-iam-add">Add member to role</button>
		</form>
		<p class="text-xs text-gray-500">Writes carry the etag shown at load time; a stale view is rejected and reloaded.</p>
	{/if}
</div>
