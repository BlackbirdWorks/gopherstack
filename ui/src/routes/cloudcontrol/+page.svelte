<script lang="ts">
	import { untrack } from 'svelte';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getCloudControlClient } from '$lib/aws-client';
	import {
		ListResourcesCommand,
		ListResourceRequestsCommand,
		GetResourceCommand,
		UpdateResourceCommand,
		CreateResourceCommand,
		GetResourceRequestStatusCommand,
		type ResourceDescription,
		type ProgressEvent
	} from '@aws-sdk/client-cloudcontrol';
	import { toast } from 'svelte-sonner';
	import { Settings, RefreshCw, Search, Box, Activity, Edit, Plus, Eye, ChevronDown, ChevronRight, Info } from 'lucide-svelte';

	const cc = regionalClient(getCloudControlClient);

	let loading = $state(false);
	let activeTab = $state<'resources' | 'requests' | 'types'>('resources');
	let searchQuery = $state('');
	let resourceType = $state('AWS::S3::Bucket');
	let resources = $state<ResourceDescription[]>([]);
	let requests = $state<ProgressEvent[]>([]);

	// Resource editor state
	let editorOpen = $state(false);
	let editorMode = $state<'create' | 'update'>('create');
	let editorResourceId = $state('');
	let editorJson = $state('{\n  \n}');
	let editorLoading = $state(false);
	let editorError = $state('');

	// Detail panel state
	let detailOpen = $state(false);
	let detailResource = $state<{ identifier: string; properties: string } | null>(null);
	let detailLoading = $state(false);
	let detailPropertiesParsed = $derived(() => {
		if (!detailResource?.properties) return null;
		try {
			return JSON.parse(detailResource.properties) as Record<string, unknown>;
		} catch {
			return null;
		}
	});

	// Request detail state
	let requestDetailOpen = $state(false);
	let requestDetail = $state<ProgressEvent | null>(null);
	let requestDetailLoading = $state(false);

	// Type introspection state
	const expandedTypes = $state<Set<string>>(new Set());

	const filteredResources = $derived(
		resources.filter((r) => (r.Identifier ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);
	const filteredRequests = $derived(
		requests.filter(
			(r) =>
				(r.ResourceModel ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(r.TypeName ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(r.Identifier ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	// Supported resource types with schema hints
	const resourceTypeOptions = [
		'AWS::S3::Bucket',
		'AWS::DynamoDB::Table',
		'AWS::Lambda::Function',
		'AWS::EC2::Instance',
		'AWS::IAM::Role',
		'AWS::SNS::Topic',
		'AWS::SQS::Queue'
	];

	// Type introspection: known properties for each supported type
	const typeSchemas: Record<string, { name: string; type: string; description: string; required?: boolean }[]> = {
		'AWS::S3::Bucket': [
			{ name: 'BucketName', type: 'string', description: 'A name for the bucket. If not specified, CloudFormation generates a unique name.', required: false },
			{ name: 'VersioningConfiguration', type: 'object', description: 'Enables multiple versions of all objects in this bucket.' },
			{ name: 'Tags', type: 'array', description: 'An arbitrary set of tags (key-value pairs) for this S3 bucket.' },
			{ name: 'AccessControl', type: 'string', description: 'A canned access control list (ACL) that grants predefined permissions to the bucket.' },
			{ name: 'BucketEncryption', type: 'object', description: 'Specifies default encryption for a bucket using server-side encryption.' },
			{ name: 'PublicAccessBlockConfiguration', type: 'object', description: 'Configuration that defines how Amazon S3 handles public access.' }
		],
		'AWS::DynamoDB::Table': [
			{ name: 'TableName', type: 'string', description: 'A name for the table. If not specified, CloudFormation generates a unique name.', required: false },
			{ name: 'AttributeDefinitions', type: 'array', description: 'A list of attributes that describe the key schema for the table and indexes.', required: true },
			{ name: 'KeySchema', type: 'array', description: 'Specifies the attributes that make up the primary key for a table.', required: true },
			{ name: 'BillingMode', type: 'string', description: 'Controls how you are charged for read and write throughput. PAY_PER_REQUEST or PROVISIONED.' },
			{ name: 'ProvisionedThroughput', type: 'object', description: 'Throughput for the specified table (required if BillingMode is PROVISIONED).' },
			{ name: 'GlobalSecondaryIndexes', type: 'array', description: 'Global secondary indexes to be created on the table.' },
			{ name: 'Tags', type: 'array', description: 'An arbitrary set of tags (key-value pairs) for this DynamoDB table.' }
		],
		'AWS::Lambda::Function': [
			{ name: 'FunctionName', type: 'string', description: 'The name of the Lambda function. If omitted, CloudFormation generates a unique name.', required: false },
			{ name: 'Runtime', type: 'string', description: 'The identifier of the function runtime (e.g., nodejs20.x, python3.12).', required: true },
			{ name: 'Handler', type: 'string', description: 'The name of the method within the code that Lambda calls to run the function.', required: true },
			{ name: 'Role', type: 'string', description: 'The Amazon Resource Name (ARN) of the IAM execution role.', required: true },
			{ name: 'Code', type: 'object', description: 'The code for the function.', required: true },
			{ name: 'MemorySize', type: 'integer', description: 'The amount of memory available to the function at runtime (MB).' },
			{ name: 'Timeout', type: 'integer', description: 'The amount of time (in seconds) that Lambda allows a function to run before stopping it.' },
			{ name: 'Environment', type: 'object', description: 'Environment variables that are accessible from function code during execution.' },
			{ name: 'Tags', type: 'array', description: 'A list of tags to apply to the function.' }
		],
		'AWS::EC2::Instance': [
			{ name: 'ImageId', type: 'string', description: 'The ID of the AMI. Required unless LaunchTemplate is specified.', required: true },
			{ name: 'InstanceType', type: 'string', description: 'The instance type (e.g., t3.micro).' },
			{ name: 'KeyName', type: 'string', description: 'The name of the key pair.' },
			{ name: 'SecurityGroupIds', type: 'array', description: 'The IDs of the security groups.' },
			{ name: 'SubnetId', type: 'string', description: 'The ID of the subnet to launch the instance into.' },
			{ name: 'Tags', type: 'array', description: 'The tags to add to the instance.' },
			{ name: 'UserData', type: 'string', description: 'The user data script to make available to the instance.' }
		],
		'AWS::IAM::Role': [
			{ name: 'RoleName', type: 'string', description: 'A name for the IAM role.', required: false },
			{ name: 'AssumeRolePolicyDocument', type: 'object', description: 'The trust policy that is associated with this role.', required: true },
			{ name: 'Description', type: 'string', description: 'A description of the role.' },
			{ name: 'ManagedPolicyArns', type: 'array', description: 'A list of ARNs of the IAM managed policies to attach to the IAM role.' },
			{ name: 'Policies', type: 'array', description: 'Adds or updates an inline policy document embedded in the specified IAM role.' },
			{ name: 'MaxSessionDuration', type: 'integer', description: 'The maximum session duration (in seconds) for the specified role.' },
			{ name: 'Tags', type: 'array', description: 'A list of tags attached to the role.' }
		],
		'AWS::SNS::Topic': [
			{ name: 'TopicName', type: 'string', description: 'The name of the topic. If not specified, CloudFormation generates a unique name.', required: false },
			{ name: 'DisplayName', type: 'string', description: 'The display name for an Amazon SNS topic with SMS subscriptions.' },
			{ name: 'Subscription', type: 'array', description: 'The SNS subscriptions associated with the topic.' },
			{ name: 'KmsMasterKeyId', type: 'string', description: 'The ID of an AWS managed customer master key (CMK) for Amazon SNS or a custom CMK.' },
			{ name: 'Tags', type: 'array', description: 'The list of tags for the topic.' }
		],
		'AWS::SQS::Queue': [
			{ name: 'QueueName', type: 'string', description: 'A name for the queue. If not specified, CloudFormation generates a unique name.', required: false },
			{ name: 'DelaySeconds', type: 'integer', description: 'The time in seconds for which the delivery of all messages in the queue is delayed (0-900).' },
			{ name: 'MessageRetentionPeriod', type: 'integer', description: 'The number of seconds that Amazon SQS retains a message (60-1209600).' },
			{ name: 'VisibilityTimeout', type: 'integer', description: 'The length of time during which a message will be unavailable after it is delivered (0-43200).' },
			{ name: 'ReceiveMessageWaitTimeSeconds', type: 'integer', description: 'Specifies the duration, in seconds, that the ReceiveMessage action call waits for a message.' },
			{ name: 'RedrivePolicy', type: 'object', description: 'A string that includes the parameters for the dead-letter queue functionality.' },
			{ name: 'KmsMasterKeyId', type: 'string', description: 'The ID of an AWS managed CMK for Amazon SQS or a custom CMK.' },
			{ name: 'Tags', type: 'array', description: 'The tags that you attach to this queue.' }
		]
	};

	function getTypeSchemaProps(typeName: string) {
		return typeSchemas[typeName] ?? [];
	}

	function toggleTypeExpanded(typeName: string) {
		if (expandedTypes.has(typeName)) {
			expandedTypes.delete(typeName);
		} else {
			expandedTypes.add(typeName);
		}
	}

	async function loadData() {
		loading = true;
		// `resourceType` is read with `untrack` so it never becomes a
		// dependency of the `onRegionChange` effect below -- the resource
		// type <select>'s onchange already calls loadData directly.
		const currentResourceType = untrack(() => resourceType);
		try {
			const [resResp, reqResp] = await Promise.all([
				cc().send(new ListResourcesCommand({ TypeName: currentResourceType })),
				cc().send(new ListResourceRequestsCommand({}))
			]);
			resources = resResp.ResourceDescriptions ?? [];
			requests = reqResp.ResourceRequestStatusSummaries ?? [];
		} catch (e) {
			toast.error('Failed to load Cloud Control data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	function openCreateEditor() {
		editorMode = 'create';
		editorResourceId = '';
		editorJson = '{\n  \n}';
		editorError = '';
		editorOpen = true;
	}

	function openUpdateEditor(identifier: string) {
		editorMode = 'update';
		editorResourceId = identifier;
		editorError = '';
		editorJson = '[\n  { "op": "replace", "path": "/PropertyName", "value": "new-value" }\n]';
		editorOpen = true;
	}

	async function submitEditor() {
		editorError = '';

		// Validate JSON
		try {
			JSON.parse(editorJson);
		} catch (e) {
			editorError = 'Invalid JSON: ' + String(e);
			return;
		}

		editorLoading = true;
		try {
			if (editorMode === 'create') {
				const resp = await cc().send(
					new CreateResourceCommand({ TypeName: resourceType, DesiredState: editorJson })
				);
				const token = resp.ProgressEvent?.RequestToken;
				toast.success(`Resource creation initiated${token ? ` (token: ${token.slice(0, 8)}…)` : ''}`);
			} else {
				await cc().send(
					new UpdateResourceCommand({
						TypeName: resourceType,
						Identifier: editorResourceId,
						PatchDocument: editorJson
					})
				);
				toast.success(`Resource "${editorResourceId}" updated`);
			}
			editorOpen = false;
			await loadData();
		} catch (e) {
			editorError = String(e);
		} finally {
			editorLoading = false;
		}
	}

	async function openResourceDetail(identifier: string) {
		detailLoading = true;
		detailOpen = true;
		detailResource = null;
		try {
			const resp = await cc().send(new GetResourceCommand({ TypeName: resourceType, Identifier: identifier }));
			const desc = resp.ResourceDescription;
			if (desc) {
				detailResource = { identifier: desc.Identifier ?? identifier, properties: desc.Properties ?? '{}' };
			}
		} catch (e) {
			toast.error('Failed to load resource: ' + String(e));
			detailOpen = false;
		} finally {
			detailLoading = false;
		}
	}

	async function openRequestDetail(requestToken: string) {
		requestDetailLoading = true;
		requestDetailOpen = true;
		requestDetail = null;
		try {
			const resp = await cc().send(new GetResourceRequestStatusCommand({ RequestToken: requestToken }));
			requestDetail = resp.ProgressEvent ?? null;
		} catch (e) {
			toast.error('Failed to load request status: ' + String(e));
			requestDetailOpen = false;
		} finally {
			requestDetailLoading = false;
		}
	}

	function statusBadgeClass(status: string | undefined) {
		switch (status) {
			case 'SUCCESS': return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400';
			case 'FAILED': return 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400';
			case 'IN_PROGRESS': return 'bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-400';
			case 'CANCEL_COMPLETE': return 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
			case 'CANCEL_IN_PROGRESS': return 'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-400';
			default: return 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
		}
	}

	function typeBadgeClass(type: string) {
		switch (type) {
			case 'string': return 'bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-400';
			case 'integer': return 'bg-purple-100 dark:bg-purple-900/30 text-purple-700 dark:text-purple-400';
			case 'object': return 'bg-orange-100 dark:bg-orange-900/30 text-orange-700 dark:text-orange-400';
			case 'array': return 'bg-teal-100 dark:bg-teal-900/30 text-teal-700 dark:text-teal-400';
			default: return 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
		}
	}

	onRegionChange(loadData);
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Settings class="w-7 h-7 text-gray-600 dark:text-gray-400" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">AWS Cloud Control API</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Manage AWS and third-party resources using a common API</p>
			</div>
		</div>
		<button onclick={loadData} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
	</div>

	<div class="grid grid-cols-2 sm:grid-cols-2 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-gray-100 dark:bg-gray-700 rounded-lg"><Box class="w-5 h-5 text-gray-600 dark:text-gray-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{resources.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Resources</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg"><Activity class="w-5 h-5 text-blue-600 dark:text-blue-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{requests.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Resource Requests</p></div>
		</div>
	</div>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between items-start sm:items-center">
			<div class="flex gap-2">
				{#each [['resources', 'Resources'], ['requests', 'Resource Requests'], ['types', 'Type Introspection']] as [tab, label]}
					<button onclick={() => { activeTab = tab as typeof activeTab; searchQuery = ''; }}
						class="px-4 py-2 rounded-lg text-sm font-medium {activeTab === tab ? 'bg-gray-600 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}">
						{label}
					</button>
				{/each}
			</div>
			<div class="flex gap-2 w-full sm:w-auto">
				{#if activeTab === 'resources'}
					<select bind:value={resourceType} onchange={loadData} class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
						{#each resourceTypeOptions as rt}
							<option value={rt}>{rt}</option>
						{/each}
					</select>
					<button onclick={openCreateEditor} class="flex items-center gap-1.5 px-3 py-2 rounded-lg bg-indigo-600 hover:bg-indigo-700 text-white text-sm font-medium">
						<Plus class="w-4 h-4" /> Create
					</button>
				{/if}
				{#if activeTab !== 'types'}
					<div class="relative">
						<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
						<input bind:value={searchQuery} placeholder="Search..." class="pl-9 pr-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white w-full sm:w-48" />
					</div>
				{/if}
			</div>
		</div>
		<div class="p-4">
			{#if loading}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading...</div>
			{:else if activeTab === 'resources'}
				{#if filteredResources.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No resources found for {resourceType}</div>
				{:else}
					<div class="space-y-2">
						{#each filteredResources as resource}
							<div class="flex items-center justify-between gap-3 p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3 min-w-0">
									<Box class="w-5 h-5 text-gray-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{resource.Identifier}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 font-mono truncate max-w-sm">{resource.Properties?.slice(0, 100)}</p>
									</div>
								</div>
								<div class="flex items-center gap-2 shrink-0">
									<button onclick={() => openResourceDetail(resource.Identifier ?? '')} title="View properties" class="p-1.5 rounded hover:bg-gray-200 dark:hover:bg-slate-600 text-gray-500 dark:text-gray-400">
										<Eye class="w-4 h-4" />
									</button>
									<button onclick={() => openUpdateEditor(resource.Identifier ?? '')} title="Edit resource" class="p-1.5 rounded hover:bg-gray-200 dark:hover:bg-slate-600 text-gray-500 dark:text-gray-400">
										<Edit class="w-4 h-4" />
									</button>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'requests'}
				{#if filteredRequests.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No resource requests found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredRequests as req}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3 min-w-0">
									<Activity class="w-5 h-5 text-blue-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white">{req.TypeName}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{req.Identifier}</p>
										{#if req.RequestToken}
											<p class="text-xs text-gray-400 dark:text-gray-500 font-mono">token: {req.RequestToken.slice(0, 16)}…</p>
										{/if}
									</div>
								</div>
								<div class="flex items-center gap-2 shrink-0">
									<span class="text-xs px-2 py-1 rounded-full {statusBadgeClass(req.OperationStatus)}">{req.OperationStatus}</span>
									<span class="text-xs px-2 py-1 rounded-full bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400">{req.Operation}</span>
									{#if req.RequestToken}
										<button onclick={() => openRequestDetail(req.RequestToken ?? '')} title="View request details" class="p-1.5 rounded hover:bg-gray-200 dark:hover:bg-slate-600 text-gray-500 dark:text-gray-400">
											<Eye class="w-4 h-4" />
										</button>
									{/if}
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'types'}
				<div class="space-y-2">
					{#each resourceTypeOptions as typeName}
						{@const props = getTypeSchemaProps(typeName)}
						{@const isExpanded = expandedTypes.has(typeName)}
						<div class="border border-slate-200 dark:border-slate-700 rounded-lg overflow-hidden">
							<button
								onclick={() => toggleTypeExpanded(typeName)}
								class="w-full flex items-center justify-between p-4 text-left hover:bg-gray-50 dark:hover:bg-slate-700/50 transition-colors"
							>
								<div class="flex items-center gap-3">
									<Info class="w-5 h-5 text-indigo-500 shrink-0" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white font-mono">{typeName}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{props.length} known properties</p>
									</div>
								</div>
								{#if isExpanded}
									<ChevronDown class="w-4 h-4 text-gray-400" />
								{:else}
									<ChevronRight class="w-4 h-4 text-gray-400" />
								{/if}
							</button>
							{#if isExpanded}
								<div class="border-t border-slate-200 dark:border-slate-700 divide-y divide-slate-100 dark:divide-slate-700/50">
									{#each props as prop}
										<div class="flex items-start gap-4 px-4 py-3 bg-slate-50 dark:bg-slate-800/50">
											<div class="min-w-0 flex-1">
												<div class="flex items-center gap-2 flex-wrap">
													<span class="font-mono text-sm font-medium text-gray-900 dark:text-white">{prop.name}</span>
													<span class="text-xs px-1.5 py-0.5 rounded {typeBadgeClass(prop.type)}">{prop.type}</span>
													{#if prop.required}
														<span class="text-xs px-1.5 py-0.5 rounded bg-red-100 dark:bg-red-900/30 text-red-600 dark:text-red-400">required</span>
													{/if}
												</div>
												<p class="text-xs text-gray-500 dark:text-gray-400 mt-1">{prop.description}</p>
											</div>
										</div>
									{/each}
								</div>
							{/if}
						</div>
					{/each}
				</div>
			{/if}
		</div>
	</div>
</div>

<!-- Resource Editor Modal (Create / Update) -->
{#if editorOpen}
	<!-- svelte-ignore a11y_click_events_have_key_events -->
	<!-- svelte-ignore a11y_no_static_element_interactions -->
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50" onclick={(e) => { if (e.target === e.currentTarget) editorOpen = false; }}>
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl w-full max-w-lg mx-4 flex flex-col gap-4 p-6">
			<div class="flex items-center justify-between">
				<h2 class="text-lg font-semibold text-gray-900 dark:text-white">
					{editorMode === 'create' ? `Create ${resourceType}` : `Update ${editorResourceId}`}
				</h2>
				<button onclick={() => editorOpen = false} class="text-gray-400 hover:text-gray-600 dark:hover:text-gray-200 text-xl leading-none">&times;</button>
			</div>

			{#if editorMode === 'create'}
				<p class="text-sm text-gray-500 dark:text-gray-400">Enter the desired state as a JSON object. Properties vary by resource type — see <em>Type Introspection</em> for field details.</p>
			{:else}
				<p class="text-sm text-gray-500 dark:text-gray-400">Enter an RFC 6902 JSON Patch document (array of <code class="font-mono">op/path/value</code> objects) to update the resource properties.</p>
			{/if}

			{#if editorError}
				<div class="text-sm text-red-600 dark:text-red-400 bg-red-50 dark:bg-red-900/20 rounded-lg p-3 font-mono whitespace-pre-wrap break-all">{editorError}</div>
			{/if}

			<textarea
				bind:value={editorJson}
				rows={10}
				spellcheck={false}
				class="w-full font-mono text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-gray-50 dark:bg-slate-900 text-gray-900 dark:text-white p-3 resize-y focus:outline-none focus:ring-2 focus:ring-indigo-500"
			></textarea>

			<div class="flex justify-end gap-3">
				<button onclick={() => editorOpen = false} class="px-4 py-2 rounded-lg text-sm text-gray-600 dark:text-gray-300 border border-gray-200 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-slate-700">Cancel</button>
				<button
					onclick={submitEditor}
					disabled={editorLoading}
					class="px-4 py-2 rounded-lg text-sm font-medium bg-indigo-600 hover:bg-indigo-700 text-white disabled:opacity-50 disabled:cursor-not-allowed"
				>
					{editorLoading ? 'Submitting…' : editorMode === 'create' ? 'Create Resource' : 'Apply Patch'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Resource Detail Modal -->
{#if detailOpen}
	<!-- svelte-ignore a11y_click_events_have_key_events -->
	<!-- svelte-ignore a11y_no_static_element_interactions -->
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50" onclick={(e) => { if (e.target === e.currentTarget) detailOpen = false; }}>
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl w-full max-w-2xl mx-4 flex flex-col gap-4 p-6 max-h-[80vh] overflow-y-auto">
			<div class="flex items-center justify-between">
				<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Resource Properties</h2>
				<button onclick={() => detailOpen = false} class="text-gray-400 hover:text-gray-600 dark:hover:text-gray-200 text-xl leading-none">&times;</button>
			</div>

			{#if detailLoading}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading…</div>
			{:else if detailResource}
				{@const parsed = detailPropertiesParsed()}
				<div class="space-y-4">
					<div>
						<p class="text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wide mb-1">Identifier</p>
						<p class="font-mono text-sm text-gray-900 dark:text-white">{detailResource.identifier}</p>
					</div>
					<div>
						<p class="text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wide mb-2">Properties</p>
						{#if parsed}
							<div class="space-y-2">
								{#each Object.entries(parsed) as [key, val]}
									<div class="flex items-start gap-3 p-2 rounded-lg bg-gray-50 dark:bg-slate-700/50">
										<span class="font-mono text-sm font-medium text-gray-700 dark:text-gray-300 shrink-0 min-w-32">{key}</span>
										<span class="font-mono text-sm text-gray-900 dark:text-white break-all">
											{typeof val === 'object' ? JSON.stringify(val) : String(val)}
										</span>
									</div>
								{/each}
							</div>
						{:else}
							<pre class="text-sm font-mono bg-gray-50 dark:bg-slate-900 rounded-lg p-3 overflow-x-auto text-gray-900 dark:text-white">{detailResource.properties}</pre>
						{/if}
					</div>
				</div>
			{/if}
		</div>
	</div>
{/if}

<!-- Request Detail Modal -->
{#if requestDetailOpen}
	<!-- svelte-ignore a11y_click_events_have_key_events -->
	<!-- svelte-ignore a11y_no_static_element_interactions -->
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50" onclick={(e) => { if (e.target === e.currentTarget) requestDetailOpen = false; }}>
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl w-full max-w-lg mx-4 flex flex-col gap-4 p-6">
			<div class="flex items-center justify-between">
				<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Request Progress Detail</h2>
				<button onclick={() => requestDetailOpen = false} class="text-gray-400 hover:text-gray-600 dark:hover:text-gray-200 text-xl leading-none">&times;</button>
			</div>

			{#if requestDetailLoading}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading…</div>
			{:else if requestDetail}
				<div class="space-y-3">
					{#each [
						['Type', requestDetail.TypeName],
						['Identifier', requestDetail.Identifier],
						['Operation', requestDetail.Operation],
						['Status', requestDetail.OperationStatus],
						['Request Token', requestDetail.RequestToken],
						['Status Message', requestDetail.StatusMessage]
					] as [label, value]}
						{#if value}
							<div class="flex items-start gap-3">
								<span class="text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wide w-32 shrink-0 pt-0.5">{label}</span>
								{#if label === 'Status'}
									<span class="text-xs px-2 py-1 rounded-full {statusBadgeClass(value)}">{value}</span>
								{:else}
									<span class="font-mono text-sm text-gray-900 dark:text-white break-all">{value}</span>
								{/if}
							</div>
						{/if}
					{/each}
				</div>
			{/if}

			<div class="flex justify-end">
				<button onclick={() => requestDetailOpen = false} class="px-4 py-2 rounded-lg text-sm text-gray-600 dark:text-gray-300 border border-gray-200 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-slate-700">Close</button>
			</div>
		</div>
	</div>
{/if}
