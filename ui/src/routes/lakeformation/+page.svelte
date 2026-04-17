<script lang="ts">
	import { onMount } from 'svelte';
	import { getLakeFormationClient } from '$lib/aws-client';
	import {
		ListResourcesCommand,
		RegisterResourceCommand,
		DeregisterResourceCommand,
		type ResourceInfo
	} from '@aws-sdk/client-lakeformation';
	import { toast } from 'svelte-sonner';
	import { Database, RefreshCw, Plus, Trash2 } from 'lucide-svelte';

	const lf = getLakeFormationClient();

	let loading = $state(false);
	let resources = $state<ResourceInfo[]>([]);
	let showRegisterModal = $state(false);
	let registering = $state(false);
	let newResourceArn = $state('');
	let newRoleArn = $state('');

	async function loadResources() {
		loading = true;
		try {
			const res = await lf.send(new ListResourcesCommand({}));
			resources = res.ResourceInfoList ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load resources: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function registerResource() {
		if (!newResourceArn.trim() || !newRoleArn.trim()) return;
		registering = true;
		try {
			await lf.send(new RegisterResourceCommand({
				ResourceArn: newResourceArn.trim(),
				RoleArn: newRoleArn.trim(),
			}));
			toast.success('Resource registered');
			showRegisterModal = false;
			newResourceArn = '';
			newRoleArn = '';
			await loadResources();
		} catch (err: unknown) {
			toast.error(`Failed to register resource: ${(err as Error).message}`);
		} finally {
			registering = false;
		}
	}

	async function deregisterResource(arn: string) {
		try {
			await lf.send(new DeregisterResourceCommand({ ResourceArn: arn }));
			toast.success('Resource deregistered');
			await loadResources();
		} catch (err: unknown) {
			toast.error(`Failed to deregister: ${(err as Error).message}`);
		}
	}

	onMount(() => { loadResources(); });
</script>

<div class="space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<div class="p-2 bg-teal-100 dark:bg-teal-900/30 rounded-lg">
				<Database class="w-6 h-6 text-teal-600 dark:text-teal-400" />
			</div>
			<div>
				<h1 class="text-3xl font-bold text-slate-900 dark:text-white">Lake Formation</h1>
				<p class="text-slate-600 dark:text-slate-300">Data lake permissions and governance</p>
			</div>
		</div>
		<div class="flex items-center gap-2">
			<button
				onclick={() => { showRegisterModal = true; }}
				class="flex items-center gap-1.5 px-3 py-2 text-sm font-medium text-white bg-teal-600 hover:bg-teal-700 rounded-lg transition-colors"
			>
				<Plus class="w-4 h-4" />
				Register Resource
			</button>
			<button onclick={() => loadResources()} class="p-2 text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white" title="Refresh">
				<RefreshCw class="w-5 h-5 {loading ? 'animate-spin' : ''}" />
			</button>
		</div>
	</div>

	{#if loading}
		<div class="flex items-center justify-center p-8">
			<svg class="w-8 h-8 animate-spin text-slate-200 dark:text-slate-600 fill-teal-600" viewBox="0 0 100 101" fill="none"><path d="M100 50.5908C100 78.2051 77.6142 100.591 50 100.591C22.3858 100.591 0 78.2051 0 50.5908C0 22.9766 22.3858 0.59082 50 0.59082C77.6142 0.59082 100 22.9766 100 50.5908ZM9.08144 50.5908C9.08144 73.1895 27.4013 91.5094 50 91.5094C72.5987 91.5094 90.9186 73.1895 90.9186 50.5908C90.9186 27.9921 72.5987 9.67226 50 9.67226C27.4013 9.67226 9.08144 27.9921 9.08144 50.5908Z" fill="currentColor" /></svg>
		</div>
	{:else if resources.length === 0}
		<div class="text-center py-12 text-slate-500">
			<Database class="w-16 h-16 mx-auto mb-4 text-slate-300 dark:text-slate-600" />
			<p class="text-lg font-medium">No resources registered</p>
			<p class="text-sm mt-1">Register an S3 location to get started</p>
		</div>
	{:else}
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 overflow-hidden">
			<table class="w-full text-sm text-left text-slate-500 dark:text-slate-400">
				<thead class="text-xs text-slate-700 uppercase bg-slate-50 dark:bg-slate-700 dark:text-slate-400">
					<tr>
						<th class="px-6 py-3">Resource ARN</th>
						<th class="px-6 py-3">Role ARN</th>
						<th class="px-6 py-3">Last Modified</th>
						<th class="px-6 py-3 text-right">Actions</th>
					</tr>
				</thead>
				<tbody>
					{#each resources as resource}
						<tr class="bg-white border-b dark:bg-slate-800 dark:border-slate-700 hover:bg-slate-50 dark:hover:bg-slate-700">
							<td class="px-6 py-4 font-medium text-slate-900 dark:text-white font-mono text-xs truncate max-w-xs" title={resource.ResourceArn}>{resource.ResourceArn}</td>
							<td class="px-6 py-4 font-mono text-xs truncate max-w-xs" title={resource.RoleArn}>{resource.RoleArn ?? '—'}</td>
							<td class="px-6 py-4">{resource.LastModified ? new Date(resource.LastModified).toLocaleDateString() : '—'}</td>
							<td class="px-6 py-4 text-right">
								<button onclick={() => deregisterResource(resource.ResourceArn ?? '')} class="text-red-600 hover:text-red-700 dark:text-red-400 dark:hover:text-red-300" title="Deregister">
									<Trash2 class="w-4 h-4" />
								</button>
							</td>
						</tr>
					{/each}
				</tbody>
			</table>
		</div>
	{/if}
</div>

{#if showRegisterModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm" onclick={(e) => { if (e.target === e.currentTarget) showRegisterModal = false; }} role="dialog" aria-modal="true">
		<div class="relative p-4 w-full max-w-md" onclick={(e) => e.stopPropagation()} role="document">
			<div class="relative bg-white rounded-lg shadow dark:bg-slate-700">
				<div class="flex items-center justify-between p-4 border-b dark:border-slate-600">
					<h3 class="text-xl font-semibold text-slate-900 dark:text-white">Register Resource</h3>
					<button onclick={() => { showRegisterModal = false; }} class="text-slate-400 bg-transparent hover:bg-slate-200 hover:text-slate-900 rounded-lg text-sm w-8 h-8 inline-flex justify-center items-center dark:hover:bg-slate-600 dark:hover:text-white"><svg class="w-3 h-3" fill="none" viewBox="0 0 14 14"><path stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m1 1 6 6m0 0 6 6M7 7l6-6M7 7l-6 6" /></svg></button>
				</div>
				<div class="p-4">
					<form class="space-y-4" onsubmit={(e) => { e.preventDefault(); registerResource(); }}>
						<div>
							<label for="resource-arn" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Resource ARN</label>
							<input type="text" id="resource-arn" bind:value={newResourceArn} placeholder="arn:aws:s3:::my-bucket" required class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:text-white" />
						</div>
						<div>
							<label for="role-arn" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Role ARN</label>
							<input type="text" id="role-arn" bind:value={newRoleArn} placeholder="arn:aws:iam::000000000000:role/my-role" required class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:text-white" />
						</div>
						<div class="flex gap-3 justify-end pt-2">
							<button type="button" onclick={() => { showRegisterModal = false; }} class="py-2 px-4 text-sm font-medium text-slate-900 bg-white rounded-lg border border-slate-200 hover:bg-slate-100 dark:bg-slate-800 dark:text-slate-400 dark:border-slate-600">Cancel</button>
							<button type="submit" disabled={registering} class="text-white bg-teal-600 hover:bg-teal-700 font-medium rounded-lg text-sm px-4 py-2 disabled:opacity-50">
								{registering ? 'Registering...' : 'Register'}
							</button>
						</div>
					</form>
				</div>
			</div>
		</div>
	</div>
{/if}
