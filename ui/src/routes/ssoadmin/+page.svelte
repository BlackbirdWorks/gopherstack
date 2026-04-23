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
		DescribePermissionSetCommand,
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

	let activeTab = $state<'instances' | 'permissionsets' | 'assignments' | 'applications' | 'tokenissuers' | 'metrics' | 'docs'>(
		'instances'
	);
	let instances = $state<Array<{ InstanceArn?: string; Name?: string; IdentityStoreId?: string }>>([]);
	let permissionSetArns = $state<string[]>([]);
	let permissionSetDetails = $state<Record<string, { Name?: string; Description?: string; SessionDuration?: string }>>({});
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

	// Metrics counters
	let metricsInstanceCount = $state(0);
	let metricsPermSetCount = $state(0);
	let metricsAppCount = $state(0);
	let metricsTTICount = $state(0);
	let metricsAssignmentCount = $state(0);

	async function loadInstances() {
		const out = await ssoadmin.send(new ListInstancesCommand({}));
		instances = out.Instances ?? [];
		if (!selectedInstanceArn && instances.length > 0) {
			selectedInstanceArn = instances[0].InstanceArn ?? '';
		}
		metricsInstanceCount = instances.length;
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
			permissionSetDetails = {};
			return;
		}
		const out = await ssoadmin.send(new ListPermissionSetsCommand({ InstanceArn: selectedInstanceArn }));
		permissionSetArns = out.PermissionSets ?? [];
		if (!selectedPermissionSetArn && permissionSetArns.length > 0) {
			selectedPermissionSetArn = permissionSetArns[0] ?? '';
		}
		metricsPermSetCount = permissionSetArns.length;

		// Load details for each permission set in parallel
		const details: Record<string, { Name?: string; Description?: string; SessionDuration?: string }> = {};
		await Promise.all(
			permissionSetArns.map(async (arn) => {
				try {
					const d = await ssoadmin.send(
						new DescribePermissionSetCommand({ InstanceArn: selectedInstanceArn, PermissionSetArn: arn })
					);
					details[arn] = {
						Name: d.PermissionSet?.Name,
						Description: d.PermissionSet?.Description,
						SessionDuration: d.PermissionSet?.SessionDuration
					};
				} catch {
					details[arn] = {};
				}
			})
		);
		permissionSetDetails = details;
	}

	async function loadAssignments() {
		if (!selectedInstanceArn || !selectedPermissionSetArn) {
			accountAssignments = [];
			return;
		}
		// AccountId is optional; omit it to list all assignments for the permission set.
		const out = await ssoadmin.send(
			new ListAccountAssignmentsCommand({
				InstanceArn: selectedInstanceArn,
				PermissionSetArn: selectedPermissionSetArn,
				AccountId: selectedAccountID || undefined
			})
		);
		accountAssignments = out.AccountAssignments ?? [];
		metricsAssignmentCount = accountAssignments.length;
	}

	async function loadApplications() {
		if (!selectedInstanceArn) {
			applications = [];
			return;
		}
		const out = await ssoadmin.send(new ListApplicationsCommand({ InstanceArn: selectedInstanceArn }));
		applications = out.Applications ?? [];
		metricsAppCount = applications.length;
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
		metricsTTICount = trustedTokenIssuers.length;
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

	async function seedDemoData() {
		try {
			// Create a demo instance
			const instResp = await ssoadmin.send(new CreateInstanceCommand({ Name: 'demo-instance' }));
			const demoInstanceArn = instResp.InstanceArn;
			if (!demoInstanceArn) return;

			// Create permission sets
			await Promise.all([
				ssoadmin.send(
					new CreatePermissionSetCommand({
						InstanceArn: demoInstanceArn,
						Name: 'AdminAccess',
						Description: 'Full administrator access'
					})
				),
				ssoadmin.send(
					new CreatePermissionSetCommand({
						InstanceArn: demoInstanceArn,
						Name: 'ReadOnlyAccess',
						Description: 'Read-only access'
					})
				)
			]);

			// Create a demo application
			await ssoadmin.send(
				new CreateApplicationCommand({
					InstanceArn: demoInstanceArn,
					Name: 'demo-app',
					ApplicationProviderArn: defaultProviderArn
				})
			);

			// Create a trusted token issuer
			await ssoadmin.send(
				new CreateTrustedTokenIssuerCommand({
					InstanceArn: demoInstanceArn,
					Name: 'demo-issuer',
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

			toast.success('Demo data seeded successfully');
			await refreshAll();
		} catch (err: unknown) {
			toast.error(`Failed to seed demo data: ${(err as Error).message}`);
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
					TargetId: selectedAccountID || defaultAccountID
				})
			);
			assignmentPrincipalID = '';
			await loadAssignments();
		} catch (err: unknown) {
			toast.error(`Failed to create assignment: ${(err as Error).message}`);
		}
	}

	async function removeAssignment(principalID: string, principalType: string, accountId: string) {
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
					TargetId: accountId || defaultAccountID
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
		<div class="flex items-start justify-between">
			<div>
				<h1 class="text-3xl font-bold text-slate-900 dark:text-white">SSO Admin</h1>
				<p class="mt-2 text-sm text-slate-600 dark:text-slate-300">
					Manage IAM Identity Center instances, permission sets, account assignments, applications, and trusted token issuers.
				</p>
			</div>
			<button
				type="button"
				onclick={() => void seedDemoData()}
				class="rounded-lg border border-indigo-300 px-4 py-2 text-sm text-indigo-600 hover:bg-indigo-50 dark:border-indigo-500 dark:text-indigo-400 dark:hover:bg-indigo-900/30"
			>
				Seed Demo Data
			</button>
		</div>
	</div>

	<div class="flex flex-wrap gap-2">
		{#each [
			['instances', 'Instances'],
			['permissionsets', 'Permission Sets'],
			['assignments', 'Assignments'],
			['applications', 'Applications'],
			['tokenissuers', 'Token Issuers'],
			['metrics', 'Metrics'],
			['docs', 'Docs']
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
			<h2 class="text-lg font-semibold text-slate-800 dark:text-slate-100">Instances</h2>
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
						<div>
							<p class="font-medium">{instance.Name}</p>
							<p class="text-xs text-slate-500 font-mono">{instance.InstanceArn}</p>
							<p class="text-xs text-slate-400">Identity Store: {instance.IdentityStoreId}</p>
						</div>
						<button
							type="button"
							class="rounded border border-red-300 px-2 py-1 text-xs text-red-600 hover:bg-red-50 dark:border-red-700 dark:text-red-400"
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
			<h2 class="text-lg font-semibold text-slate-800 dark:text-slate-100">Permission Sets</h2>
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
						<div>
							<p class="font-medium">{permissionSetDetails[permissionSetArn]?.Name ?? '—'}</p>
							<p class="text-xs text-slate-500 font-mono">{permissionSetArn}</p>
							{#if permissionSetDetails[permissionSetArn]?.Description}
								<p class="text-xs text-slate-400">{permissionSetDetails[permissionSetArn].Description}</p>
							{/if}
							<p class="text-xs text-slate-400">Session: {permissionSetDetails[permissionSetArn]?.SessionDuration ?? 'PT1H'}</p>
						</div>
						<div class="flex gap-2">
							<button type="button" class="rounded border px-2 py-1 text-xs" onclick={() => void provisionPermissionSet(permissionSetArn)}>
								Provision
							</button>
							<button type="button" class="rounded border border-red-300 px-2 py-1 text-xs text-red-600 hover:bg-red-50 dark:border-red-700 dark:text-red-400" onclick={() => void removePermissionSet(permissionSetArn)}>
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
			<h2 class="text-lg font-semibold text-slate-800 dark:text-slate-100">Account Assignments</h2>
			<div class="grid gap-2 md:grid-cols-2 lg:grid-cols-4">
				<div>
					<label class="text-xs text-slate-500">Permission Set</label>
					<select bind:value={selectedPermissionSetArn} class="mt-1 w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-700 dark:bg-slate-900">
						<option value="">Select permission set</option>
						{#each permissionSetArns as permissionSetArn}
							<option value={permissionSetArn}>{permissionSetDetails[permissionSetArn]?.Name ?? permissionSetArn}</option>
						{/each}
					</select>
				</div>
				<div>
					<label class="text-xs text-slate-500">Account ID (optional)</label>
					<input bind:value={selectedAccountID} placeholder="123456789012" class="mt-1 w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-700 dark:bg-slate-900" />
				</div>
				<div>
					<label class="text-xs text-slate-500">Principal Type</label>
					<select bind:value={assignmentPrincipalType} class="mt-1 w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-700 dark:bg-slate-900">
						<option value="USER">USER</option>
						<option value="GROUP">GROUP</option>
					</select>
				</div>
				<div>
					<label class="text-xs text-slate-500">Principal ID</label>
					<input bind:value={assignmentPrincipalID} placeholder="Principal ID" class="mt-1 w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-700 dark:bg-slate-900" />
				</div>
			</div>
			<button type="button" class="rounded-lg bg-indigo-600 px-4 py-2 text-sm text-white" onclick={createAssignment}>Create Assignment</button>
			<ul class="space-y-2 text-sm">
				{#each accountAssignments as assignment}
					<li class="flex items-center justify-between rounded-lg border border-slate-200 p-3 dark:border-slate-700">
						<div>
							<p class="font-medium">{assignment.PrincipalType}: {assignment.PrincipalId}</p>
							<p class="text-xs text-slate-500">Account: {assignment.AccountId}</p>
						</div>
						<button
							type="button"
							class="rounded border border-red-300 px-2 py-1 text-xs text-red-600 hover:bg-red-50 dark:border-red-700 dark:text-red-400"
							onclick={() => void removeAssignment(assignment.PrincipalId ?? '', assignment.PrincipalType ?? 'USER', assignment.AccountId ?? defaultAccountID)}
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
			<h2 class="text-lg font-semibold text-slate-800 dark:text-slate-100">Applications</h2>
			<div class="flex gap-2">
				<input bind:value={newApplicationName} placeholder="Application name" class="flex-1 rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-700 dark:bg-slate-900" />
				<button type="button" class="rounded-lg bg-indigo-600 px-4 py-2 text-sm text-white" onclick={createApplication}>Create</button>
			</div>
			<ul class="space-y-2 text-sm">
				{#each applications as application}
					<li class="flex items-center justify-between rounded-lg border border-slate-200 p-3 dark:border-slate-700">
						<div>
							<p class="font-medium">{application.Name}</p>
							<p class="text-xs text-slate-500 font-mono">{application.ApplicationArn}</p>
							<span class={`inline-block mt-1 rounded px-2 py-0.5 text-xs ${application.Status === 'ENABLED' ? 'bg-green-100 text-green-700 dark:bg-green-900/40 dark:text-green-400' : 'bg-slate-100 text-slate-600 dark:bg-slate-700 dark:text-slate-300'}`}>
								{application.Status}
							</span>
						</div>
						<button type="button" class="rounded border border-red-300 px-2 py-1 text-xs text-red-600 hover:bg-red-50 dark:border-red-700 dark:text-red-400" onclick={() => void removeApplication(application.ApplicationArn ?? '')}>
							Delete
						</button>
					</li>
				{/each}
			</ul>
		</div>
	{/if}

	{#if activeTab === 'tokenissuers'}
		<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800 space-y-4">
			<h2 class="text-lg font-semibold text-slate-800 dark:text-slate-100">Trusted Token Issuers</h2>
			<div class="flex gap-2">
				<input bind:value={newTrustedTokenIssuerName} placeholder="Trusted token issuer name" class="flex-1 rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-700 dark:bg-slate-900" />
				<button type="button" class="rounded-lg bg-indigo-600 px-4 py-2 text-sm text-white" onclick={createTrustedTokenIssuer}>Create</button>
			</div>
			<ul class="space-y-2 text-sm">
				{#each trustedTokenIssuers as trustedTokenIssuer}
					<li class="flex items-center justify-between rounded-lg border border-slate-200 p-3 dark:border-slate-700">
						<div>
							<p class="font-medium">{trustedTokenIssuer.Name}</p>
							<p class="text-xs text-slate-500 font-mono">{trustedTokenIssuer.TrustedTokenIssuerArn}</p>
							<p class="text-xs text-slate-400">Type: {trustedTokenIssuer.TrustedTokenIssuerType}</p>
						</div>
						<button
							type="button"
							class="rounded border border-red-300 px-2 py-1 text-xs text-red-600 hover:bg-red-50 dark:border-red-700 dark:text-red-400"
							onclick={() => void removeTrustedTokenIssuer(trustedTokenIssuer.TrustedTokenIssuerArn ?? '')}
						>
							Delete
						</button>
					</li>
				{/each}
			</ul>
		</div>
	{/if}

	{#if activeTab === 'metrics'}
		<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800 space-y-4">
			<h2 class="text-lg font-semibold text-slate-800 dark:text-slate-100">Metrics</h2>
			<div class="grid gap-4 sm:grid-cols-2 lg:grid-cols-5">
				{#each [
					['Instances', metricsInstanceCount, 'bg-blue-50 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300'],
					['Permission Sets', metricsPermSetCount, 'bg-purple-50 text-purple-700 dark:bg-purple-900/30 dark:text-purple-300'],
					['Applications', metricsAppCount, 'bg-green-50 text-green-700 dark:bg-green-900/30 dark:text-green-300'],
					['Token Issuers', metricsTTICount, 'bg-orange-50 text-orange-700 dark:bg-orange-900/30 dark:text-orange-300'],
					['Assignments', metricsAssignmentCount, 'bg-rose-50 text-rose-700 dark:bg-rose-900/30 dark:text-rose-300']
				] as [label, count, cls] (label)}
					<div class={`rounded-xl p-4 ${cls}`}>
						<p class="text-sm font-medium">{label}</p>
						<p class="mt-1 text-3xl font-bold">{count}</p>
					</div>
				{/each}
			</div>
			<button type="button" class="rounded-lg border border-slate-300 px-4 py-2 text-sm" onclick={() => void refreshAll()}>
				Refresh Metrics
			</button>
		</div>
	{/if}

	{#if activeTab === 'docs'}
		<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800 space-y-6">
			<h2 class="text-lg font-semibold text-slate-800 dark:text-slate-100">API Documentation</h2>
			<p class="text-sm text-slate-600 dark:text-slate-300">
				This service emulates AWS IAM Identity Center (SSO Admin). The following operations are supported:
			</p>

			{#each [
				{
					category: 'Instances',
					ops: [
						{ name: 'CreateInstance', desc: 'Create a new SSO instance.' },
						{ name: 'DescribeInstance', desc: 'Retrieve details for a specific SSO instance.' },
						{ name: 'ListInstances', desc: 'List all SSO instances.' },
						{ name: 'DeleteInstance', desc: 'Delete an SSO instance and all its dependent resources.' },
						{ name: 'UpdateInstance', desc: 'Update the name of an SSO instance.' },
						{ name: 'AddRegion', desc: 'Add a region to an SSO instance.' },
						{ name: 'RemoveRegion', desc: 'Remove a region from an SSO instance.' },
						{ name: 'ListRegions', desc: 'List regions associated with an SSO instance.' }
					]
				},
				{
					category: 'Permission Sets',
					ops: [
						{ name: 'CreatePermissionSet', desc: 'Create a permission set within an instance.' },
						{ name: 'DescribePermissionSet', desc: 'Get details of a permission set.' },
						{ name: 'UpdatePermissionSet', desc: 'Update a permission set name, description, or session duration.' },
						{ name: 'DeletePermissionSet', desc: 'Delete a permission set and cascade-delete its assignments.' },
						{ name: 'ListPermissionSets', desc: 'List all permission sets for an instance.' },
						{ name: 'ProvisionPermissionSet', desc: 'Provision a permission set to accounts.' },
						{ name: 'AttachManagedPolicyToPermissionSet', desc: 'Attach an AWS managed policy.' },
						{ name: 'DetachManagedPolicyFromPermissionSet', desc: 'Detach an AWS managed policy.' },
						{ name: 'ListManagedPoliciesInPermissionSet', desc: 'List AWS managed policies.' },
						{ name: 'AttachCustomerManagedPolicyReferenceToPermissionSet', desc: 'Attach a customer-managed policy reference.' },
						{ name: 'DetachCustomerManagedPolicyReferenceFromPermissionSet', desc: 'Detach a customer-managed policy reference.' },
						{ name: 'ListCustomerManagedPolicyReferencesInPermissionSet', desc: 'List customer-managed policy references.' },
						{ name: 'PutInlinePolicyToPermissionSet', desc: 'Set an inline policy on a permission set.' },
						{ name: 'GetInlinePolicyForPermissionSet', desc: 'Get the inline policy of a permission set.' },
						{ name: 'DeleteInlinePolicyFromPermissionSet', desc: 'Remove the inline policy from a permission set.' },
						{ name: 'PutPermissionsBoundaryToPermissionSet', desc: 'Set a permissions boundary on a permission set.' },
						{ name: 'GetPermissionsBoundaryForPermissionSet', desc: 'Get the permissions boundary of a permission set.' },
						{ name: 'DeletePermissionsBoundaryFromPermissionSet', desc: 'Remove the permissions boundary from a permission set.' }
					]
				},
				{
					category: 'Account Assignments',
					ops: [
						{ name: 'CreateAccountAssignment', desc: 'Assign a permission set to a principal in an account. Idempotent.' },
						{ name: 'DeleteAccountAssignment', desc: 'Remove an account assignment.' },
						{ name: 'ListAccountAssignments', desc: 'List account assignments for a permission set, optionally filtered by account ID.' },
						{ name: 'DescribeAccountAssignmentCreationStatus', desc: 'Get the status of an assignment creation request.' },
						{ name: 'DescribeAccountAssignmentDeletionStatus', desc: 'Get the status of an assignment deletion request.' },
						{ name: 'ListAccountAssignmentCreationStatus', desc: 'List account assignment creation statuses.' },
						{ name: 'ListAccountAssignmentDeletionStatus', desc: 'List account assignment deletion statuses.' }
					]
				},
				{
					category: 'Applications',
					ops: [
						{ name: 'CreateApplication', desc: 'Create an application within an instance.' },
						{ name: 'DescribeApplication', desc: 'Get details of an application.' },
						{ name: 'UpdateApplication', desc: 'Update an application name, description, or status.' },
						{ name: 'DeleteApplication', desc: 'Delete an application.' },
						{ name: 'ListApplications', desc: 'List applications for an instance (sorted by ARN).' },
						{ name: 'CreateApplicationAssignment', desc: 'Assign a principal to an application.' },
						{ name: 'DeleteApplicationAssignment', desc: 'Remove an application assignment.' },
						{ name: 'DescribeApplicationAssignment', desc: 'Describe a specific application assignment.' },
						{ name: 'ListApplicationAssignments', desc: 'List assignments for an application (sorted).' },
						{ name: 'PutApplicationAccessScope', desc: 'Add an access scope to an application.' },
						{ name: 'DeleteApplicationAccessScope', desc: 'Remove an access scope.' },
						{ name: 'ListApplicationAccessScopes', desc: 'List access scopes for an application.' },
						{ name: 'PutApplicationAuthenticationMethod', desc: 'Add an authentication method.' },
						{ name: 'DeleteApplicationAuthenticationMethod', desc: 'Remove an authentication method.' },
						{ name: 'ListApplicationAuthenticationMethods', desc: 'List authentication methods.' },
						{ name: 'PutApplicationGrant', desc: 'Add a grant type to an application.' },
						{ name: 'DeleteApplicationGrant', desc: 'Remove a grant type.' },
						{ name: 'ListApplicationGrants', desc: 'List grant types for an application.' },
						{ name: 'PutApplicationAssignmentConfiguration', desc: 'Configure whether assignments are required.' },
						{ name: 'GetApplicationAssignmentConfiguration', desc: 'Get assignment configuration for an application.' },
						{ name: 'PutApplicationSessionConfiguration', desc: 'Set the session duration for an application.' },
						{ name: 'GetApplicationSessionConfiguration', desc: 'Get the session configuration for an application.' },
						{ name: 'ListApplicationProviders', desc: 'List application providers.' },
						{ name: 'DescribeApplicationProvider', desc: 'Describe an application provider.' }
					]
				},
				{
					category: 'Trusted Token Issuers',
					ops: [
						{ name: 'CreateTrustedTokenIssuer', desc: 'Create a trusted token issuer (defaults to OIDC_JWT type).' },
						{ name: 'DescribeTrustedTokenIssuer', desc: 'Get details of a trusted token issuer.' },
						{ name: 'UpdateTrustedTokenIssuer', desc: 'Update a trusted token issuer name or type.' },
						{ name: 'DeleteTrustedTokenIssuer', desc: 'Delete a trusted token issuer.' },
						{ name: 'ListTrustedTokenIssuers', desc: 'List trusted token issuers for an instance (sorted by ARN).' }
					]
				},
				{
					category: 'ABAC / Access Control Attributes',
					ops: [
						{ name: 'CreateInstanceAccessControlAttributeConfiguration', desc: 'Create ABAC configuration for an instance.' },
						{ name: 'DescribeInstanceAccessControlAttributeConfiguration', desc: 'Get ABAC configuration.' },
						{ name: 'UpdateInstanceAccessControlAttributeConfiguration', desc: 'Update ABAC configuration.' },
						{ name: 'DeleteInstanceAccessControlAttributeConfiguration', desc: 'Delete ABAC configuration.' }
					]
				},
				{
					category: 'Tags',
					ops: [
						{ name: 'TagResource', desc: 'Add tags to an instance, permission set, application, or trusted token issuer.' },
						{ name: 'UntagResource', desc: 'Remove tags from a resource.' },
						{ name: 'ListTagsForResource', desc: 'List all tags on a resource.' }
					]
				}
			] as section}
				<div>
					<h3 class="mb-2 text-sm font-semibold text-slate-700 dark:text-slate-200">{section.category}</h3>
					<ul class="space-y-1">
						{#each section.ops as op}
							<li class="flex gap-3 text-sm">
								<span class="w-64 shrink-0 font-mono text-indigo-600 dark:text-indigo-400">{op.name}</span>
								<span class="text-slate-600 dark:text-slate-300">{op.desc}</span>
							</li>
						{/each}
					</ul>
				</div>
			{/each}
		</div>
	{/if}
</div>

