<script lang="ts">
	import { onMount } from 'svelte';
	import {
		CreateAccountAssignmentCommand,
		CreateApplicationCommand,
		CreateInstanceCommand,
		CreatePermissionSetCommand,
		CreateTrustedTokenIssuerCommand,
		DeleteAccountAssignmentCommand,
		DeleteApplicationCommand,
		DeleteInstanceCommand,
		DeletePermissionSetCommand,
		DeleteTrustedTokenIssuerCommand,
		DescribeInstanceCommand,
		ListAccountAssignmentsCommand,
		ListApplicationsCommand,
		ListInstancesCommand,
		ListPermissionSetsCommand,
		ListTrustedTokenIssuersCommand,
		ProvisionPermissionSetCommand
	} from '@aws-sdk/client-sso-admin';
	import { toast } from 'svelte-sonner';

	import { getSSOAdminClient } from '$lib/aws-client';

	const ssoadmin = getSSOAdminClient();
	const defaultAccountID = '123456789012';
	const defaultProviderArn = 'arn:aws:sso::123456789012:applicationProvider/custom';

	let activeTab = $state<'instances' | 'permissionsets' | 'assignments' | 'applications' | 'tokenissuers'>(
		'instances'
	);
	let instances = $state<Array<{ InstanceArn?: string; Name?: string; IdentityStoreId?: string }>>([]);
	let permissionSetArns = $state<string[]>([]);
	let accountAssignments = $state<Array<{ PrincipalId?: string; PrincipalType?: string; AccountId?: string }>>([]);
	let applications = $state<Array<{ ApplicationArn?: string; Name?: string; Status?: string }>>([]);
	let trustedTokenIssuers = $state<Array<{ TrustedTokenIssuerArn?: string; Name?: string; TrustedTokenIssuerType?: string }>>([]);

	let selectedInstanceArn = $state('');
	let selectedPermissionSetArn = $state('');
	let selectedAccountID = $state(defaultAccountID);
	let instanceDetails = $state<{ Name?: string; IdentityStoreId?: string; OwnerAccountId?: string } | null>(null);

	let newInstanceName = $state('');
	let newPermissionSetName = $state('');
	let assignmentPrincipalID = $state('');
	let assignmentPrincipalType = $state<'USER' | 'GROUP'>('USER');
	let newApplicationName = $state('');
	let newTrustedTokenIssuerName = $state('');

	async function loadInstances() {
		const out = await ssoadmin.send(new ListInstancesCommand({}));
		instances = out.Instances ?? [];
		if (!selectedInstanceArn && instances.length > 0) {
			selectedInstanceArn = instances[0].InstanceArn ?? '';
		}
	}

	async function describeInstance(instanceArn: string) {
		if (!instanceArn) {
			instanceDetails = null;
			return;
		}
		const out = await ssoadmin.send(new DescribeInstanceCommand({ InstanceArn: instanceArn }));
		instanceDetails = out.InstanceArn
			? {
					Name: out.Name,
					IdentityStoreId: out.IdentityStoreId,
					OwnerAccountId: out.OwnerAccountId
				}
			: null;
	}

	async function loadPermissionSets() {
		if (!selectedInstanceArn) {
			permissionSetArns = [];
			return;
		}
		const out = await ssoadmin.send(new ListPermissionSetsCommand({ InstanceArn: selectedInstanceArn }));
		permissionSetArns = out.PermissionSets ?? [];
		if (!selectedPermissionSetArn && permissionSetArns.length > 0) {
			selectedPermissionSetArn = permissionSetArns[0] ?? '';
		}
	}

	async function loadAssignments() {
		if (!selectedInstanceArn || !selectedPermissionSetArn) {
			accountAssignments = [];
			return;
		}
		const out = await ssoadmin.send(
			new ListAccountAssignmentsCommand({
				InstanceArn: selectedInstanceArn,
				PermissionSetArn: selectedPermissionSetArn,
				AccountId: selectedAccountID
			})
		);
		accountAssignments = out.AccountAssignments ?? [];
	}

	async function loadApplications() {
		if (!selectedInstanceArn) {
			applications = [];
			return;
		}
		const out = await ssoadmin.send(new ListApplicationsCommand({ InstanceArn: selectedInstanceArn }));
		applications = out.Applications ?? [];
	}

	async function loadTrustedTokenIssuers() {
		if (!selectedInstanceArn) {
			trustedTokenIssuers = [];
			return;
		}
		const out = await ssoadmin.send(
			new ListTrustedTokenIssuersCommand({
				InstanceArn: selectedInstanceArn
			})
		);
		trustedTokenIssuers = out.TrustedTokenIssuers ?? [];
	}

	async function refreshAll() {
		try {
			await loadInstances();
			await Promise.all([
				describeInstance(selectedInstanceArn),
				loadPermissionSets(),
				loadApplications(),
				loadTrustedTokenIssuers()
			]);
			await loadAssignments();
		} catch (err: unknown) {
			toast.error(`Failed to load SSO Admin data: ${(err as Error).message}`);
		}
	}

	async function createInstance() {
		if (!newInstanceName.trim()) {
			toast.error('Instance name is required');
			return;
		}
		try {
			await ssoadmin.send(new CreateInstanceCommand({ Name: newInstanceName.trim() }));
			newInstanceName = '';
			await refreshAll();
		} catch (err: unknown) {
			toast.error(`Failed to create instance: ${(err as Error).message}`);
		}
	}

	async function removeInstance(instanceArn: string) {
		try {
			await ssoadmin.send(new DeleteInstanceCommand({ InstanceArn: instanceArn }));
			if (selectedInstanceArn === instanceArn) {
				selectedInstanceArn = '';
				selectedPermissionSetArn = '';
			}
			await refreshAll();
		} catch (err: unknown) {
			toast.error(`Failed to delete instance: ${(err as Error).message}`);
		}
	}

	async function createPermissionSet() {
		if (!selectedInstanceArn || !newPermissionSetName.trim()) {
			toast.error('Instance and permission set name are required');
			return;
		}
		try {
			await ssoadmin.send(
				new CreatePermissionSetCommand({
					InstanceArn: selectedInstanceArn,
					Name: newPermissionSetName.trim()
				})
			);
			newPermissionSetName = '';
			await loadPermissionSets();
		} catch (err: unknown) {
			toast.error(`Failed to create permission set: ${(err as Error).message}`);
		}
	}

	async function removePermissionSet(permissionSetArn: string) {
		if (!selectedInstanceArn) {
			return;
		}
		try {
			await ssoadmin.send(
				new DeletePermissionSetCommand({
					InstanceArn: selectedInstanceArn,
					PermissionSetArn: permissionSetArn
				})
			);
			if (selectedPermissionSetArn === permissionSetArn) {
				selectedPermissionSetArn = '';
			}
			await loadPermissionSets();
			await loadAssignments();
		} catch (err: unknown) {
			toast.error(`Failed to delete permission set: ${(err as Error).message}`);
		}
	}

	async function provisionPermissionSet(permissionSetArn: string) {
		if (!selectedInstanceArn) {
			return;
		}
		try {
			await ssoadmin.send(
				new ProvisionPermissionSetCommand({
					InstanceArn: selectedInstanceArn,
					PermissionSetArn: permissionSetArn,
					TargetType: 'ALL_PROVISIONED_ACCOUNTS'
				})
			);
			toast.success('Provisioning request created');
		} catch (err: unknown) {
			toast.error(`Failed to provision permission set: ${(err as Error).message}`);
		}
	}

	async function createAssignment() {
		if (!selectedInstanceArn || !selectedPermissionSetArn || !assignmentPrincipalID.trim()) {
			toast.error('Instance, permission set, and principal are required');
			return;
		}
		try {
			await ssoadmin.send(
				new CreateAccountAssignmentCommand({
					InstanceArn: selectedInstanceArn,
					PermissionSetArn: selectedPermissionSetArn,
					PrincipalType: assignmentPrincipalType,
					PrincipalId: assignmentPrincipalID.trim(),
					TargetType: 'AWS_ACCOUNT',
					TargetId: selectedAccountID
				})
			);
			assignmentPrincipalID = '';
			await loadAssignments();
		} catch (err: unknown) {
			toast.error(`Failed to create assignment: ${(err as Error).message}`);
		}
	}

	async function removeAssignment(principalID: string, principalType: string) {
		if (!selectedInstanceArn || !selectedPermissionSetArn) {
			return;
		}
		try {
			await ssoadmin.send(
				new DeleteAccountAssignmentCommand({
					InstanceArn: selectedInstanceArn,
					PermissionSetArn: selectedPermissionSetArn,
					PrincipalType: principalType as 'USER' | 'GROUP',
					PrincipalId: principalID,
					TargetType: 'AWS_ACCOUNT',
					TargetId: selectedAccountID
				})
			);
			await loadAssignments();
		} catch (err: unknown) {
			toast.error(`Failed to delete assignment: ${(err as Error).message}`);
		}
	}

	async function createApplication() {
		if (!selectedInstanceArn || !newApplicationName.trim()) {
			toast.error('Instance and application name are required');
			return;
		}
		try {
			await ssoadmin.send(
				new CreateApplicationCommand({
					InstanceArn: selectedInstanceArn,
					Name: newApplicationName.trim(),
					ApplicationProviderArn: defaultProviderArn
				})
			);
			newApplicationName = '';
			await loadApplications();
		} catch (err: unknown) {
			toast.error(`Failed to create application: ${(err as Error).message}`);
		}
	}

	async function removeApplication(applicationArn: string) {
		try {
			await ssoadmin.send(new DeleteApplicationCommand({ ApplicationArn: applicationArn }));
			await loadApplications();
		} catch (err: unknown) {
			toast.error(`Failed to delete application: ${(err as Error).message}`);
		}
	}

	async function createTrustedTokenIssuer() {
		if (!selectedInstanceArn || !newTrustedTokenIssuerName.trim()) {
			toast.error('Instance and issuer name are required');
			return;
		}
		try {
			await ssoadmin.send(
				new CreateTrustedTokenIssuerCommand({
					InstanceArn: selectedInstanceArn,
					Name: newTrustedTokenIssuerName.trim(),
					TrustedTokenIssuerType: 'OIDC_JWT',
					TrustedTokenIssuerConfiguration: {
						OidcJwtConfiguration: {
							IssuerUrl: 'https://issuer.example.com',
							ClaimAttributePath: 'sub',
							IdentityStoreAttributePath: 'UserName',
							JwksRetrievalOption: 'OPEN_ID_DISCOVERY'
						}
					}
				})
			);
			newTrustedTokenIssuerName = '';
			await loadTrustedTokenIssuers();
		} catch (err: unknown) {
			toast.error(`Failed to create trusted token issuer: ${(err as Error).message}`);
		}
	}

	async function removeTrustedTokenIssuer(trustedTokenIssuerArn: string) {
		try {
			await ssoadmin.send(new DeleteTrustedTokenIssuerCommand({ TrustedTokenIssuerArn: trustedTokenIssuerArn }));
			await loadTrustedTokenIssuers();
		} catch (err: unknown) {
			toast.error(`Failed to delete trusted token issuer: ${(err as Error).message}`);
		}
	}

	$effect(() => {
		if (selectedInstanceArn) {
			void describeInstance(selectedInstanceArn);
			void loadPermissionSets();
			void loadApplications();
			void loadTrustedTokenIssuers();
		}
	});

	$effect(() => {
		if (selectedInstanceArn && selectedPermissionSetArn) {
			void loadAssignments();
		}
	});

	onMount(() => {
		void refreshAll();
	});
</script>

<div class="space-y-6 p-6">
	<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
		<h1 class="text-3xl font-bold text-slate-900 dark:text-white">SSO Admin</h1>
		<p class="mt-2 text-sm text-slate-600 dark:text-slate-300">
			Instances, permission sets, account assignments, applications, and trusted token issuers
		</p>
	</div>

	<div class="flex flex-wrap gap-2">
		{#each [
			['instances', 'Instances'],
			['permissionsets', 'Permission Sets'],
			['assignments', 'Assignments'],
			['applications', 'Applications'],
			['tokenissuers', 'Trusted Token Issuers']
		] as [tabId, label] (tabId)}
			<button
				type="button"
				onclick={() => (activeTab = tabId as typeof activeTab)}
				class={`rounded-lg border px-4 py-2 text-sm ${activeTab === tabId ? 'bg-indigo-600 text-white border-indigo-600' : 'border-slate-300 dark:border-slate-700'}`}
			>
				{label}
			</button>
		{/each}
	</div>

	<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800 space-y-4">
		<label class="text-sm text-slate-600 dark:text-slate-300">
			Instance
			<select
				bind:value={selectedInstanceArn}
				class="mt-1 w-full rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm dark:border-slate-700 dark:bg-slate-900"
			>
				<option value="">Select instance</option>
				{#each instances as instance}
					<option value={instance.InstanceArn}>{instance.Name} ({instance.InstanceArn})</option>
				{/each}
			</select>
		</label>
		{#if instanceDetails}
			<p class="text-xs text-slate-500 dark:text-slate-400">
				Identity Store: {instanceDetails.IdentityStoreId} · Owner: {instanceDetails.OwnerAccountId}
			</p>
		{/if}
	</div>

	{#if activeTab === 'instances'}
		<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800 space-y-4">
			<div class="flex gap-2">
				<input
					bind:value={newInstanceName}
					placeholder="Instance name"
					class="flex-1 rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-700 dark:bg-slate-900"
				/>
				<button type="button" class="rounded-lg bg-indigo-600 px-4 py-2 text-sm text-white" onclick={createInstance}>
					Create
				</button>
			</div>
			<ul class="space-y-2 text-sm">
				{#each instances as instance}
					<li class="flex items-center justify-between rounded-lg border border-slate-200 p-3 dark:border-slate-700">
						<div>{instance.Name} — {instance.InstanceArn}</div>
						<button
							type="button"
							class="rounded border px-2 py-1 text-xs"
							onclick={() => void removeInstance(instance.InstanceArn ?? '')}
						>
							Delete
						</button>
					</li>
				{/each}
			</ul>
		</div>
	{/if}

	{#if activeTab === 'permissionsets'}
		<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800 space-y-4">
			<div class="flex gap-2">
				<input
					bind:value={newPermissionSetName}
					placeholder="Permission set name"
					class="flex-1 rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-700 dark:bg-slate-900"
				/>
				<button type="button" class="rounded-lg bg-indigo-600 px-4 py-2 text-sm text-white" onclick={createPermissionSet}>
					Create
				</button>
			</div>
			<ul class="space-y-2 text-sm">
				{#each permissionSetArns as permissionSetArn}
					<li class="flex items-center justify-between rounded-lg border border-slate-200 p-3 dark:border-slate-700">
						<div>{permissionSetArn}</div>
						<div class="flex gap-2">
							<button type="button" class="rounded border px-2 py-1 text-xs" onclick={() => void provisionPermissionSet(permissionSetArn)}>
								Provision
							</button>
							<button type="button" class="rounded border px-2 py-1 text-xs" onclick={() => void removePermissionSet(permissionSetArn)}>
								Delete
							</button>
						</div>
					</li>
				{/each}
			</ul>
		</div>
	{/if}

	{#if activeTab === 'assignments'}
		<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800 space-y-4">
			<div class="grid gap-2 md:grid-cols-4">
				<select bind:value={selectedPermissionSetArn} class="rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-700 dark:bg-slate-900">
					<option value="">Select permission set</option>
					{#each permissionSetArns as permissionSetArn}
						<option value={permissionSetArn}>{permissionSetArn}</option>
					{/each}
				</select>
				<input bind:value={selectedAccountID} class="rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-700 dark:bg-slate-900" />
				<select bind:value={assignmentPrincipalType} class="rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-700 dark:bg-slate-900">
					<option value="USER">USER</option>
					<option value="GROUP">GROUP</option>
				</select>
				<input bind:value={assignmentPrincipalID} placeholder="Principal ID" class="rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-700 dark:bg-slate-900" />
			</div>
			<button type="button" class="rounded-lg bg-indigo-600 px-4 py-2 text-sm text-white" onclick={createAssignment}>Create Assignment</button>
			<ul class="space-y-2 text-sm">
				{#each accountAssignments as assignment}
					<li class="flex items-center justify-between rounded-lg border border-slate-200 p-3 dark:border-slate-700">
						<div>{assignment.PrincipalType} {assignment.PrincipalId} on {assignment.AccountId}</div>
						<button
							type="button"
							class="rounded border px-2 py-1 text-xs"
							onclick={() => void removeAssignment(assignment.PrincipalId ?? '', assignment.PrincipalType ?? 'USER')}
						>
							Delete
						</button>
					</li>
				{/each}
			</ul>
		</div>
	{/if}

	{#if activeTab === 'applications'}
		<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800 space-y-4">
			<div class="flex gap-2">
				<input bind:value={newApplicationName} placeholder="Application name" class="flex-1 rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-700 dark:bg-slate-900" />
				<button type="button" class="rounded-lg bg-indigo-600 px-4 py-2 text-sm text-white" onclick={createApplication}>Create</button>
			</div>
			<ul class="space-y-2 text-sm">
				{#each applications as application}
					<li class="flex items-center justify-between rounded-lg border border-slate-200 p-3 dark:border-slate-700">
						<div>{application.Name} ({application.Status})</div>
						<button type="button" class="rounded border px-2 py-1 text-xs" onclick={() => void removeApplication(application.ApplicationArn ?? '')}>
							Delete
						</button>
					</li>
				{/each}
			</ul>
		</div>
	{/if}

	{#if activeTab === 'tokenissuers'}
		<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800 space-y-4">
			<div class="flex gap-2">
				<input bind:value={newTrustedTokenIssuerName} placeholder="Trusted token issuer name" class="flex-1 rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-700 dark:bg-slate-900" />
				<button type="button" class="rounded-lg bg-indigo-600 px-4 py-2 text-sm text-white" onclick={createTrustedTokenIssuer}>Create</button>
			</div>
			<ul class="space-y-2 text-sm">
				{#each trustedTokenIssuers as trustedTokenIssuer}
					<li class="flex items-center justify-between rounded-lg border border-slate-200 p-3 dark:border-slate-700">
						<div>{trustedTokenIssuer.Name} ({trustedTokenIssuer.TrustedTokenIssuerType})</div>
						<button
							type="button"
							class="rounded border px-2 py-1 text-xs"
							onclick={() => void removeTrustedTokenIssuer(trustedTokenIssuer.TrustedTokenIssuerArn ?? '')}
						>
							Delete
						</button>
					</li>
				{/each}
			</ul>
		</div>
	{/if}
</div>
