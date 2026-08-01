<script lang="ts">
	import { untrack } from 'svelte';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import {
		Globe,
		Plus,
		Trash2,
		RefreshCw,
		ChevronRight,
		Search,
		Database,
		Link,
		Lock,
		Zap,
		BookOpen,
		BarChart2,
		Network,
		Copy,
		Check,
	} from 'lucide-svelte';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { getAPIGatewayV2Client } from '$lib/aws-client';
	import {
		CreateApiCommand,
		DeleteApiCommand,
		GetApiCommand,
		GetApisCommand,
		UpdateApiCommand,
		CreateStageCommand,
		DeleteStageCommand,
		GetStagesCommand,
		UpdateStageCommand,
		CreateRouteCommand,
		DeleteRouteCommand,
		GetRoutesCommand,
		UpdateRouteCommand,
		CreateIntegrationCommand,
		DeleteIntegrationCommand,
		GetIntegrationsCommand,
		CreateAuthorizerCommand,
		DeleteAuthorizerCommand,
		GetAuthorizersCommand,
		CreateDeploymentCommand,
		GetDeploymentsCommand,
		DeleteDeploymentCommand,
		CreateDomainNameCommand,
		DeleteDomainNameCommand,
		GetDomainNamesCommand,
		CreateVpcLinkCommand,
		DeleteVpcLinkCommand,
		GetVpcLinksCommand,
		TagResourceCommand,
		GetTagsCommand,
	} from '@aws-sdk/client-apigatewayv2';
	import type {
		Api,
		Stage,
		Route,
		Integration,
		Authorizer,
		Deployment,
		DomainName,
		VpcLink,
	} from '@aws-sdk/client-apigatewayv2';

	const apigwv2 = regionalClient(getAPIGatewayV2Client);

	// ─── Top-level state ──────────────────────────────────────────────────────────
	type TopTab = 'apis' | 'domainnames' | 'vpclinkstab' | 'metrics' | 'docs';
	let topTab = $state<TopTab>('apis');
	let loading = $state(false);
	let creating = $state(false);
	let searchQuery = $state('');

	// ─── HTTP APIs ────────────────────────────────────────────────────────────────
	let apis = $state<Api[]>([]);
	let selectedApi = $state<Api | null>(null);
	let loadingApiDetail = $state(false);
	type ApiDetailTab = 'routes' | 'stages' | 'integrations' | 'authorizers' | 'deployments';
	let apiDetailTab = $state<ApiDetailTab>('routes');

	// Detail sub-resources
	let apiRoutes = $state<Route[]>([]);
	let apiStages = $state<Stage[]>([]);
	let apiIntegrations = $state<Integration[]>([]);
	let apiAuthorizers = $state<Authorizer[]>([]);
	let apiDeployments = $state<Deployment[]>([]);

	// Create API modal
	let showCreateApiModal = $state(false);
	let newApiName = $state('');
	let newApiDescription = $state('');
	let newApiProtocol = $state<'HTTP' | 'WEBSOCKET'>('HTTP');

	// Create Route modal
	let showCreateRouteModal = $state(false);
	let newRouteKey = $state('GET /items');
	let newRouteTarget = $state('');

	// Create Stage modal
	let showCreateStageModal = $state(false);
	let newStageName = $state('');
	let newStageDescription = $state('');
	let newStageAutoDeploy = $state(false);

	// Create Integration modal
	let showCreateIntegModal = $state(false);
	let newIntegType = $state<'AWS_PROXY' | 'HTTP_PROXY' | 'HTTP' | 'MOCK'>('AWS_PROXY');
	let newIntegUri = $state('');
	let newIntegMethod = $state('POST');

	// Create Authorizer modal
	let showCreateAuthModal = $state(false);
	let newAuthName = $state('');
	let newAuthType = $state<'JWT' | 'REQUEST'>('JWT');
	let newAuthIdentitySource = $state('$request.header.Authorization');
	let newAuthIssuer = $state('');
	let newAuthAudience = $state('');

	// Deploy modal
	let showDeployModal = $state(false);
	let deployStageName = $state('$default');
	let deployDescription = $state('');
	let deploying = $state(false);

	// Copied state
	let copiedId = $state<string | null>(null);

	const filteredApis = $derived(
		apis.filter(
			(a) =>
				(a.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(a.Description ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	// ─── Domain Names ─────────────────────────────────────────────────────────────
	let domainNames = $state<DomainName[]>([]);
	let showCreateDomainModal = $state(false);
	let newDomainName = $state('');
	let newDomainCertArn = $state('');

	const filteredDomains = $derived(
		domainNames.filter((d) => (d.DomainName ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	// ─── VPC Links ────────────────────────────────────────────────────────────────
	let vpcLinks = $state<VpcLink[]>([]);
	let showCreateVpcLinkModal = $state(false);
	let newVpcLinkName = $state('');
	let newVpcLinkSubnets = $state('');

	const filteredVpcLinks = $derived(
		vpcLinks.filter((v) => (v.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	// ─── Metrics ─────────────────────────────────────────────────────────────────
	const metrics = $derived({
		totalApis: apis.length,
		httpApis: apis.filter((a) => a.ProtocolType === 'HTTP').length,
		wsApis: apis.filter((a) => a.ProtocolType === 'WEBSOCKET').length,
		totalRoutes: apiRoutes.length,
		totalIntegrations: apiIntegrations.length,
		totalStages: apiStages.length,
		totalAuthorizers: apiAuthorizers.length,
		totalDeployments: apiDeployments.length,
		totalDomains: domainNames.length,
		totalVpcLinks: vpcLinks.length,
	});

	// ─── Supported operations list ────────────────────────────────────────────────
	const supportedOperations = [
		'CreateApi', 'DeleteApi', 'GetApi', 'GetApis', 'UpdateApi',
		'CreateStage', 'DeleteStage', 'GetStage', 'GetStages', 'UpdateStage',
		'CreateRoute', 'DeleteRoute', 'GetRoute', 'GetRoutes', 'UpdateRoute',
		'CreateIntegration', 'DeleteIntegration', 'GetIntegration', 'GetIntegrations', 'UpdateIntegration',
		'CreateAuthorizer', 'DeleteAuthorizer', 'GetAuthorizer', 'GetAuthorizers', 'UpdateAuthorizer',
		'CreateDeployment', 'DeleteDeployment', 'GetDeployment', 'GetDeployments', 'UpdateDeployment',
		'CreateDomainName', 'DeleteDomainName', 'GetDomainName', 'GetDomainNames', 'UpdateDomainName',
		'CreateApiMapping', 'DeleteApiMapping', 'GetApiMapping', 'GetApiMappings', 'UpdateApiMapping',
		'CreateVpcLink', 'DeleteVpcLink', 'GetVpcLink', 'GetVpcLinks', 'UpdateVpcLink',
		'CreateIntegrationResponse', 'DeleteIntegrationResponse', 'GetIntegrationResponse', 'GetIntegrationResponses', 'UpdateIntegrationResponse',
		'CreateModel', 'DeleteModel', 'GetModel', 'GetModels', 'UpdateModel', 'GetModelTemplate',
		'CreateRouteResponse', 'DeleteRouteResponse', 'GetRouteResponse', 'GetRouteResponses', 'UpdateRouteResponse',
		'GetTags', 'TagResource', 'UntagResource',
		'ReimportApi', 'ExportApi', 'ImportApi',
		'DeleteAccessLogSettings', 'DeleteCorsConfiguration',
		'DeleteRouteRequestParameter', 'DeleteRouteSettings',
		'ResetAuthorizersCache',
		'CreateRoutingRule', 'DeleteRoutingRule', 'GetRoutingRule', 'ListRoutingRules', 'PutRoutingRule',
		'GetPortalProductSharingPolicy', 'PutPortalProductSharingPolicy', 'DeletePortalProductSharingPolicy',
		'DisablePortal', 'PublishPortal',
	];

	// ─── Data loaders ─────────────────────────────────────────────────────────────
	async function loadApis() {
		loading = true;
		try {
			const res = await apigwv2().send(new GetApisCommand({}));
			apis = res.Items ?? [];
		} catch (e) {
			toast.error(`Failed to load APIs: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function loadApiDetail(api: Api) {
		// Every create/delete handler for a sub-resource (route, stage,
		// integration, authorizer, deployment) refreshes by re-calling this
		// same function with the already-selected API, to pick up the new
		// state. Only reset the detail tab back to "routes" when this is a
		// genuinely new selection -- otherwise creating e.g. a stage while on
		// the Stages tab would silently punt the user back to Routes and hide
		// the resource they just created.
		const isNewSelection = selectedApi?.ApiId !== api.ApiId;
		selectedApi = api;
		if (!api.ApiId) return;
		loadingApiDetail = true;
		apiRoutes = [];
		apiStages = [];
		apiIntegrations = [];
		apiAuthorizers = [];
		apiDeployments = [];
		if (isNewSelection) apiDetailTab = 'routes';
		try {
			const [routesRes, stagesRes, integrationsRes, authorizersRes, deploymentsRes] =
				await Promise.all([
					apigwv2().send(new GetRoutesCommand({ ApiId: api.ApiId })),
					apigwv2().send(new GetStagesCommand({ ApiId: api.ApiId })),
					apigwv2().send(new GetIntegrationsCommand({ ApiId: api.ApiId })),
					apigwv2().send(new GetAuthorizersCommand({ ApiId: api.ApiId })),
					apigwv2().send(new GetDeploymentsCommand({ ApiId: api.ApiId })),
				]);
			apiRoutes = routesRes.Items ?? [];
			apiStages = stagesRes.Items ?? [];
			apiIntegrations = integrationsRes.Items ?? [];
			apiAuthorizers = authorizersRes.Items ?? [];
			apiDeployments = deploymentsRes.Items ?? [];
		} catch (e) {
			toast.error(`Failed to load API details: ${e}`);
		} finally {
			loadingApiDetail = false;
		}
	}

	async function loadDomainNames() {
		loading = true;
		try {
			const res = await apigwv2().send(new GetDomainNamesCommand({}));
			domainNames = res.Items ?? [];
		} catch (e) {
			toast.error(`Failed to load domain names: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function loadVpcLinks() {
		loading = true;
		try {
			const res = await apigwv2().send(new GetVpcLinksCommand({}));
			vpcLinks = res.Items ?? [];
		} catch (e) {
			toast.error(`Failed to load VPC links: ${e}`);
		} finally {
			loading = false;
		}
	}

	// ─── CRUD ─────────────────────────────────────────────────────────────────────
	async function createApi() {
		if (!newApiName.trim()) return;
		creating = true;
		try {
			await apigwv2().send(
				new CreateApiCommand({
					Name: newApiName.trim(),
					Description: newApiDescription.trim() || undefined,
					ProtocolType: newApiProtocol,
				})
			);
			toast.success(`HTTP API "${newApiName}" created`);
			showCreateApiModal = false;
			newApiName = '';
			newApiDescription = '';
			await loadApis();
		} catch (e) {
			toast.error(`Failed to create API: ${e}`);
		} finally {
			creating = false;
		}
	}

	async function deleteApi(api: Api) {
		if (
			!api.ApiId ||
			!(await confirmDestructive({ title: 'Delete API', message: `Delete API "${api.Name}"?` }))
		)
			return;
		try {
			await apigwv2().send(new DeleteApiCommand({ ApiId: api.ApiId }));
			toast.success(`API "${api.Name}" deleted`);
			if (selectedApi?.ApiId === api.ApiId) selectedApi = null;
			await loadApis();
		} catch (e) {
			toast.error(`Failed to delete API: ${e}`);
		}
	}

	async function createRoute() {
		if (!selectedApi?.ApiId || !newRouteKey.trim()) return;
		creating = true;
		try {
			await apigwv2().send(
				new CreateRouteCommand({
					ApiId: selectedApi.ApiId,
					RouteKey: newRouteKey.trim(),
					Target: newRouteTarget.trim() || undefined,
				})
			);
			toast.success(`Route "${newRouteKey}" created`);
			showCreateRouteModal = false;
			newRouteKey = 'GET /items';
			newRouteTarget = '';
			await loadApiDetail(selectedApi);
		} catch (e) {
			toast.error(`Failed to create route: ${e}`);
		} finally {
			creating = false;
		}
	}

	async function deleteRoute(route: Route) {
		if (!selectedApi?.ApiId || !route.RouteId) return;
		if (!(await confirmDestructive({ title: 'Delete Route', message: `Delete route "${route.RouteKey}"?` })))
			return;
		try {
			await apigwv2().send(new DeleteRouteCommand({ ApiId: selectedApi.ApiId, RouteId: route.RouteId }));
			toast.success(`Route "${route.RouteKey}" deleted`);
			await loadApiDetail(selectedApi);
		} catch (e) {
			toast.error(`Failed to delete route: ${e}`);
		}
	}

	async function createStage() {
		if (!selectedApi?.ApiId || !newStageName.trim()) return;
		creating = true;
		try {
			await apigwv2().send(
				new CreateStageCommand({
					ApiId: selectedApi.ApiId,
					StageName: newStageName.trim(),
					Description: newStageDescription.trim() || undefined,
					AutoDeploy: newStageAutoDeploy,
				})
			);
			toast.success(`Stage "${newStageName}" created`);
			showCreateStageModal = false;
			newStageName = '';
			newStageDescription = '';
			newStageAutoDeploy = false;
			await loadApiDetail(selectedApi);
		} catch (e) {
			toast.error(`Failed to create stage: ${e}`);
		} finally {
			creating = false;
		}
	}

	async function deleteStage(stage: Stage) {
		if (!selectedApi?.ApiId || !stage.StageName) return;
		if (
			!(await confirmDestructive({
				title: 'Delete Stage',
				message: `Delete stage "${stage.StageName}"?`,
			}))
		)
			return;
		try {
			await apigwv2().send(
				new DeleteStageCommand({ ApiId: selectedApi.ApiId, StageName: stage.StageName })
			);
			toast.success(`Stage "${stage.StageName}" deleted`);
			await loadApiDetail(selectedApi);
		} catch (e) {
			toast.error(`Failed to delete stage: ${e}`);
		}
	}

	async function createIntegration() {
		if (!selectedApi?.ApiId) return;
		creating = true;
		try {
			await apigwv2().send(
				new CreateIntegrationCommand({
					ApiId: selectedApi.ApiId,
					IntegrationType: newIntegType,
					IntegrationUri: newIntegUri.trim() || undefined,
					IntegrationMethod: newIntegMethod.trim() || undefined,
				})
			);
			toast.success(`Integration (${newIntegType}) created`);
			showCreateIntegModal = false;
			newIntegUri = '';
			await loadApiDetail(selectedApi);
		} catch (e) {
			toast.error(`Failed to create integration: ${e}`);
		} finally {
			creating = false;
		}
	}

	async function deleteIntegration(integ: Integration) {
		if (!selectedApi?.ApiId || !integ.IntegrationId) return;
		if (
			!(await confirmDestructive({
				title: 'Delete Integration',
				message: `Delete integration "${integ.IntegrationId}"?`,
			}))
		)
			return;
		try {
			await apigwv2().send(
				new DeleteIntegrationCommand({ ApiId: selectedApi.ApiId, IntegrationId: integ.IntegrationId })
			);
			toast.success('Integration deleted');
			await loadApiDetail(selectedApi);
		} catch (e) {
			toast.error(`Failed to delete integration: ${e}`);
		}
	}

	async function createAuthorizer() {
		if (!selectedApi?.ApiId || !newAuthName.trim()) return;
		creating = true;
		try {
			await apigwv2().send(
				new CreateAuthorizerCommand({
					ApiId: selectedApi.ApiId,
					Name: newAuthName.trim(),
					AuthorizerType: newAuthType,
					IdentitySource: newAuthIdentitySource.trim() ? [newAuthIdentitySource.trim()] : undefined,
					JwtConfiguration:
						newAuthType === 'JWT' && (newAuthIssuer || newAuthAudience)
							? {
									Issuer: newAuthIssuer.trim() || undefined,
									Audience: newAuthAudience ? [newAuthAudience.trim()] : undefined,
								}
							: undefined,
				})
			);
			toast.success(`Authorizer "${newAuthName}" created`);
			showCreateAuthModal = false;
			newAuthName = '';
			newAuthIssuer = '';
			newAuthAudience = '';
			await loadApiDetail(selectedApi);
		} catch (e) {
			toast.error(`Failed to create authorizer: ${e}`);
		} finally {
			creating = false;
		}
	}

	async function deleteAuthorizer(auth: Authorizer) {
		if (!selectedApi?.ApiId || !auth.AuthorizerId) return;
		if (
			!(await confirmDestructive({
				title: 'Delete Authorizer',
				message: `Delete authorizer "${auth.Name}"?`,
			}))
		)
			return;
		try {
			await apigwv2().send(
				new DeleteAuthorizerCommand({ ApiId: selectedApi.ApiId, AuthorizerId: auth.AuthorizerId })
			);
			toast.success(`Authorizer "${auth.Name}" deleted`);
			await loadApiDetail(selectedApi);
		} catch (e) {
			toast.error(`Failed to delete authorizer: ${e}`);
		}
	}

	async function deployApi() {
		if (!selectedApi?.ApiId || !deployStageName.trim()) return;
		deploying = true;
		try {
			await apigwv2().send(
				new CreateDeploymentCommand({
					ApiId: selectedApi.ApiId,
					StageName: deployStageName.trim() || undefined,
					Description: deployDescription.trim() || undefined,
				})
			);
			toast.success(`Deployed to stage "${deployStageName}"`);
			showDeployModal = false;
			deployStageName = '$default';
			deployDescription = '';
			await loadApiDetail(selectedApi);
		} catch (e) {
			toast.error(`Deploy failed: ${e}`);
		} finally {
			deploying = false;
		}
	}

	async function deleteDeployment(depl: Deployment) {
		if (!selectedApi?.ApiId || !depl.DeploymentId) return;
		if (!(await confirmDestructive({ title: 'Delete Deployment', message: `Delete deployment ${depl.DeploymentId}?` })))
			return;
		try {
			await apigwv2().send(
				new DeleteDeploymentCommand({ ApiId: selectedApi.ApiId, DeploymentId: depl.DeploymentId })
			);
			toast.success('Deployment deleted');
			await loadApiDetail(selectedApi);
		} catch (e) {
			toast.error(`Failed to delete deployment: ${e}`);
		}
	}

	async function createDomainName() {
		if (!newDomainName.trim()) return;
		creating = true;
		try {
			await apigwv2().send(
				new CreateDomainNameCommand({
					DomainName: newDomainName.trim(),
					DomainNameConfigurations: newDomainCertArn
						? [{ CertificateArn: newDomainCertArn.trim(), EndpointType: 'REGIONAL' }]
						: undefined,
				})
			);
			toast.success(`Domain "${newDomainName}" created`);
			showCreateDomainModal = false;
			newDomainName = '';
			newDomainCertArn = '';
			await loadDomainNames();
		} catch (e) {
			toast.error(`Failed to create domain: ${e}`);
		} finally {
			creating = false;
		}
	}

	async function deleteDomainName(dn: DomainName) {
		if (
			!dn.DomainName ||
			!(await confirmDestructive({ title: 'Delete Domain', message: `Delete domain "${dn.DomainName}"?` }))
		)
			return;
		try {
			await apigwv2().send(new DeleteDomainNameCommand({ DomainName: dn.DomainName }));
			toast.success(`Domain "${dn.DomainName}" deleted`);
			await loadDomainNames();
		} catch (e) {
			toast.error(`Failed to delete domain: ${e}`);
		}
	}

	async function createVpcLink() {
		if (!newVpcLinkName.trim()) return;
		creating = true;
		try {
			const subnets = newVpcLinkSubnets.split(',').map((s) => s.trim()).filter(Boolean);
			await apigwv2().send(
				new CreateVpcLinkCommand({
					Name: newVpcLinkName.trim(),
					SubnetIds: subnets.length > 0 ? subnets : ['subnet-default'],
				})
			);
			toast.success(`VPC Link "${newVpcLinkName}" created`);
			showCreateVpcLinkModal = false;
			newVpcLinkName = '';
			newVpcLinkSubnets = '';
			await loadVpcLinks();
		} catch (e) {
			toast.error(`Failed to create VPC link: ${e}`);
		} finally {
			creating = false;
		}
	}

	async function deleteVpcLink(vl: VpcLink) {
		if (
			!vl.VpcLinkId ||
			!(await confirmDestructive({ title: 'Delete VPC Link', message: `Delete VPC link "${vl.Name}"?` }))
		)
			return;
		try {
			await apigwv2().send(new DeleteVpcLinkCommand({ VpcLinkId: vl.VpcLinkId }));
			toast.success(`VPC Link "${vl.Name}" deleted`);
			await loadVpcLinks();
		} catch (e) {
			toast.error(`Failed to delete VPC link: ${e}`);
		}
	}

	// ─── Demo data ────────────────────────────────────────────────────────────────
	async function seedDemoData() {
		creating = true;
		try {
			// Create a demo HTTP API
			const httpApi = await apigwv2().send(
				new CreateApiCommand({
					Name: 'petstore-http-api',
					ProtocolType: 'HTTP',
					Description: 'Demo PetStore HTTP API',
					CorsConfiguration: {
						AllowOrigins: ['https://example.com'],
						AllowMethods: ['GET', 'POST', 'DELETE'],
						AllowHeaders: ['Content-Type', 'Authorization'],
					},
				})
			);
			if (httpApi.ApiId) {
				// Create a default stage with auto-deploy
				await apigwv2().send(
					new CreateStageCommand({
						ApiId: httpApi.ApiId,
						StageName: '$default',
						AutoDeploy: true,
					})
				);
				// Create routes
				await apigwv2().send(
					new CreateRouteCommand({ ApiId: httpApi.ApiId, RouteKey: 'GET /pets' })
				);
				await apigwv2().send(
					new CreateRouteCommand({ ApiId: httpApi.ApiId, RouteKey: 'POST /pets' })
				);
				await apigwv2().send(
					new CreateRouteCommand({ ApiId: httpApi.ApiId, RouteKey: 'DELETE /pets/{petId}' })
				);
				// Create an AWS_PROXY integration
				await apigwv2().send(
					new CreateIntegrationCommand({
						ApiId: httpApi.ApiId,
						IntegrationType: 'AWS_PROXY',
						IntegrationUri:
							'arn:aws:lambda:us-east-1:123456789012:function:petstore-handler',
						PayloadFormatVersion: '2.0',
					})
				);
				// Create a JWT authorizer
				await apigwv2().send(
					new CreateAuthorizerCommand({
						ApiId: httpApi.ApiId,
						Name: 'cognito-jwt',
						AuthorizerType: 'JWT',
						IdentitySource: ['$request.header.Authorization'],
						JwtConfiguration: {
							Issuer: 'https://cognito-idp.us-east-1.amazonaws.com/us-east-1_EXAMPLE',
							Audience: ['my-app-client'],
						},
					})
				);
				// Tag the API
				await apigwv2().send(
					new TagResourceCommand({
						ResourceArn: `arn:aws:apigateway:us-east-1::/apis/${httpApi.ApiId}`,
						Tags: { env: 'demo', team: 'platform' },
					})
				);
			}

			// Create a demo WebSocket API
			const wsApi = await apigwv2().send(
				new CreateApiCommand({
					Name: 'chat-websocket-api',
					ProtocolType: 'WEBSOCKET',
					Description: 'Demo chat WebSocket API',
				})
			);
			if (wsApi.ApiId) {
				await apigwv2().send(
					new CreateRouteCommand({ ApiId: wsApi.ApiId, RouteKey: '$connect' })
				);
				await apigwv2().send(
					new CreateRouteCommand({ ApiId: wsApi.ApiId, RouteKey: '$disconnect' })
				);
				await apigwv2().send(
					new CreateRouteCommand({ ApiId: wsApi.ApiId, RouteKey: 'sendmessage' })
				);
				await apigwv2().send(
					new CreateStageCommand({
						ApiId: wsApi.ApiId,
						StageName: 'prod',
						Description: 'Production stage',
					})
				);
			}

			// Create a demo domain
			await apigwv2().send(
				new CreateDomainNameCommand({
					DomainName: 'api.demo.example.com',
					DomainNameConfigurations: [
						{ CertificateArn: 'arn:aws:acm:us-east-1:123:certificate/demo', EndpointType: 'REGIONAL' },
					],
				})
			);

			// Create a demo VPC link
			await apigwv2().send(
				new CreateVpcLinkCommand({
					Name: 'private-backend-link',
					SubnetIds: ['subnet-aaa111', 'subnet-bbb222'],
					Tags: { env: 'demo' },
				})
			);

			toast.success('Demo data seeded');
			await loadApis();
		} catch (e) {
			toast.error(`Demo seed failed: ${e}`);
		} finally {
			creating = false;
		}
	}

	async function copyToClipboard(text: string, id: string) {
		await navigator.clipboard.writeText(text);
		copiedId = id;
		setTimeout(() => {
			copiedId = null;
		}, 2000);
	}

	async function switchTopTab(tab: TopTab) {
		topTab = tab;
		searchQuery = '';
		selectedApi = null;
		if (tab === 'apis') await loadApis();
		else if (tab === 'domainnames') await loadDomainNames();
		else if (tab === 'vpclinkstab') await loadVpcLinks();
	}

	// Only one top-level tab's data is loaded at a time; on a region change
	// reset everything region-scoped and reload whichever tab is active.
	// `topTab` is read via untrack() because switchTopTab() also writes it:
	// without untrack, every tab switch would re-trigger this region effect
	// and double-fetch.
	onRegionChange(() => {
		apis = [];
		selectedApi = null;
		apiRoutes = [];
		apiStages = [];
		apiIntegrations = [];
		apiAuthorizers = [];
		apiDeployments = [];
		domainNames = [];
		vpcLinks = [];
		const tab = untrack(() => topTab);
		if (tab === 'apis') void loadApis();
		else if (tab === 'domainnames') void loadDomainNames();
		else if (tab === 'vpclinkstab') void loadVpcLinks();
	});
</script>

<div class="space-y-4">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Zap class="h-8 w-8 text-purple-600" />
			<div>
				<h1 class="text-2xl font-bold">API Gateway V2</h1>
				<p class="text-sm text-muted-foreground">Create and manage HTTP and WebSocket APIs</p>
			</div>
		</div>
		<div class="flex gap-2">
			<button
				onclick={seedDemoData}
				disabled={creating}
				class="flex items-center gap-2 rounded-md border px-3 py-2 text-sm hover:bg-accent disabled:opacity-50"
				title="Seed demo data"
			>
				<Database class="h-4 w-4" />
				Demo Data
			</button>
			<button
				onclick={() => switchTopTab(topTab)}
				class="flex items-center gap-2 rounded-md border px-3 py-2 text-sm hover:bg-accent"
			>
				<RefreshCw class="h-4 w-4" />
				Refresh
			</button>
		</div>
	</div>

	<!-- Top Tabs -->
	<div class="flex border-b overflow-x-auto">
		<button
			onclick={() => switchTopTab('apis')}
			class="flex items-center gap-1.5 whitespace-nowrap px-4 py-2 text-sm font-medium border-b-2 transition-colors {topTab === 'apis' ? 'border-primary text-primary' : 'border-transparent text-muted-foreground hover:text-foreground'}"
		>
			<Globe class="h-4 w-4" />HTTP APIs
		</button>
		<button
			onclick={() => switchTopTab('domainnames')}
			class="flex items-center gap-1.5 whitespace-nowrap px-4 py-2 text-sm font-medium border-b-2 transition-colors {topTab === 'domainnames' ? 'border-primary text-primary' : 'border-transparent text-muted-foreground hover:text-foreground'}"
		>
			<Link class="h-4 w-4" />Domain Names
		</button>
		<button
			onclick={() => switchTopTab('vpclinkstab')}
			class="flex items-center gap-1.5 whitespace-nowrap px-4 py-2 text-sm font-medium border-b-2 transition-colors {topTab === 'vpclinkstab' ? 'border-primary text-primary' : 'border-transparent text-muted-foreground hover:text-foreground'}"
		>
			<Network class="h-4 w-4" />VPC Links
		</button>
		<button
			onclick={() => switchTopTab('metrics')}
			class="flex items-center gap-1.5 whitespace-nowrap px-4 py-2 text-sm font-medium border-b-2 transition-colors {topTab === 'metrics' ? 'border-primary text-primary' : 'border-transparent text-muted-foreground hover:text-foreground'}"
		>
			<BarChart2 class="h-4 w-4" />Metrics
		</button>
		<button
			onclick={() => switchTopTab('docs')}
			class="flex items-center gap-1.5 whitespace-nowrap px-4 py-2 text-sm font-medium border-b-2 transition-colors {topTab === 'docs' ? 'border-primary text-primary' : 'border-transparent text-muted-foreground hover:text-foreground'}"
		>
			<BookOpen class="h-4 w-4" />Docs
		</button>
	</div>

	<!-- ═══════════════════════════════ APIs Tab ══════════════════════════════════ -->
	{#if topTab === 'apis'}
		<div class="flex items-center justify-between gap-4">
			<div class="relative flex-1">
				<Search class="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
				<input
					type="text"
					placeholder="Search APIs…"
					bind:value={searchQuery}
					class="w-full rounded-md border bg-background pl-9 pr-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
				/>
			</div>
			<button
				onclick={() => (showCreateApiModal = true)}
				class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90"
			>
				<Plus class="h-4 w-4" />
				Create API
			</button>
		</div>

		{#if loading}
			<div class="flex justify-center py-8">
				<RefreshCw class="h-6 w-6 animate-spin text-muted-foreground" />
			</div>
		{:else if filteredApis.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-center">
				<Zap class="mb-3 h-12 w-12 text-muted-foreground/40" />
				<p class="text-muted-foreground">No HTTP APIs found</p>
				<button
					onclick={() => (showCreateApiModal = true)}
					class="mt-3 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90"
				>Create your first API</button>
			</div>
		{:else}
			<div class="grid gap-3">
				{#each filteredApis as api (api.ApiId)}
					<div
						class="flex cursor-pointer items-center justify-between rounded-lg border bg-card p-4 transition-colors hover:bg-accent/50 {selectedApi?.ApiId === api.ApiId ? 'ring-2 ring-primary' : ''}"
						role="button"
						tabindex="0"
						onclick={() => loadApiDetail(api)}
						onkeydown={(e) => {
							if (e.key === 'Enter' || e.key === ' ') {
								e.preventDefault();
								loadApiDetail(api);
							}
						}}
					>
						<div class="flex items-center gap-3 min-w-0">
							<div class="flex h-9 w-9 flex-shrink-0 items-center justify-center rounded-md bg-purple-100 dark:bg-purple-900/30">
								<Zap class="h-5 w-5 text-purple-600 dark:text-purple-400" />
							</div>
							<div class="min-w-0">
								<div class="flex items-center gap-2">
									<p class="font-medium truncate">{api.Name}</p>
									<span class="rounded-full px-2 py-0.5 text-xs font-semibold {api.ProtocolType === 'WEBSOCKET' ? 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-300' : 'bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300'}">
										{api.ProtocolType}
									</span>
								</div>
								<p class="text-xs text-muted-foreground truncate">{api.Description ?? 'No description'}</p>
								<div class="flex items-center gap-1 mt-0.5">
									<p class="text-xs font-mono text-muted-foreground truncate">{api.ApiEndpoint ?? ''}</p>
									{#if api.ApiEndpoint}
										<button
											onclick={(e) => { e.stopPropagation(); copyToClipboard(api.ApiEndpoint!, api.ApiId!); }}
											class="p-0.5 hover:text-foreground text-muted-foreground"
											title="Copy endpoint"
										>
											{#if copiedId === api.ApiId}
												<Check class="h-3 w-3" />
											{:else}
												<Copy class="h-3 w-3" />
											{/if}
										</button>
									{/if}
								</div>
							</div>
						</div>
						<div class="flex items-center gap-2 ml-2">
							<ChevronRight class="h-4 w-4 text-muted-foreground" />
							<button
								onclick={(e) => { e.stopPropagation(); deleteApi(api); }}
								class="rounded-md p-1.5 text-muted-foreground hover:bg-destructive/10 hover:text-destructive"
							>
								<Trash2 class="h-4 w-4" />
							</button>
						</div>
					</div>
				{/each}
			</div>
		{/if}

		<!-- API Detail Panel -->
		{#if selectedApi}
			<div class="mt-4 rounded-lg border bg-card">
				<div class="flex items-center justify-between border-b p-4">
					<div>
						<h2 class="font-semibold">{selectedApi.Name}</h2>
						<p class="text-xs text-muted-foreground font-mono">{selectedApi.ApiId}</p>
					</div>
					<div class="flex gap-2">
						<button
							onclick={() => (showDeployModal = true)}
							class="flex items-center gap-2 rounded-md bg-green-600 px-3 py-1.5 text-sm text-white hover:bg-green-700"
						>
							<Zap class="h-3.5 w-3.5" />
							Deploy
						</button>
					</div>
				</div>

				<!-- API Detail Tabs -->
				<div class="flex border-b overflow-x-auto">
					{#each [['routes', 'Routes'], ['stages', 'Stages'], ['integrations', 'Integrations'], ['authorizers', 'Authorizers'], ['deployments', 'Deployments']] as [tabId, label] (tabId)}
						<button
							onclick={() => (apiDetailTab = tabId as ApiDetailTab)}
							class="whitespace-nowrap px-4 py-2 text-sm font-medium border-b-2 transition-colors {apiDetailTab === tabId ? 'border-primary text-primary' : 'border-transparent text-muted-foreground hover:text-foreground'}"
						>
							{label}
						</button>
					{/each}
				</div>

				{#if loadingApiDetail}
					<div class="flex justify-center py-8">
						<RefreshCw class="h-6 w-6 animate-spin text-muted-foreground" />
					</div>
				{:else}
					<div class="p-4">
						<!-- Routes Tab -->
						{#if apiDetailTab === 'routes'}
							<div class="mb-3 flex justify-end">
								<button
									onclick={() => (showCreateRouteModal = true)}
									class="flex items-center gap-1.5 rounded-md bg-primary px-3 py-1.5 text-xs text-primary-foreground hover:bg-primary/90"
								>
									<Plus class="h-3.5 w-3.5" />
									Add Route
								</button>
							</div>
							{#if apiRoutes.length === 0}
								<p class="text-center text-sm text-muted-foreground py-4">No routes configured</p>
							{:else}
								<div class="space-y-2">
									{#each apiRoutes as route (route.RouteId)}
										<div class="flex items-center justify-between rounded-md border bg-background p-3">
											<div>
												<p class="text-sm font-mono font-medium">{route.RouteKey}</p>
												{#if route.Target}
													<p class="text-xs text-muted-foreground">→ {route.Target}</p>
												{/if}
												{#if route.AuthorizationType && route.AuthorizationType !== 'NONE'}
													<span class="mt-1 inline-flex items-center gap-1 rounded-full bg-amber-100 px-2 py-0.5 text-xs text-amber-700 dark:bg-amber-900/30 dark:text-amber-300">
														<Lock class="h-3 w-3" />
														{route.AuthorizationType}
													</span>
												{/if}
											</div>
											<button
												onclick={() => deleteRoute(route)}
												class="rounded-md p-1.5 text-muted-foreground hover:bg-destructive/10 hover:text-destructive"
											>
												<Trash2 class="h-4 w-4" />
											</button>
										</div>
									{/each}
								</div>
							{/if}
						{/if}

						<!-- Stages Tab -->
						{#if apiDetailTab === 'stages'}
							<div class="mb-3 flex justify-end">
								<button
									onclick={() => (showCreateStageModal = true)}
									class="flex items-center gap-1.5 rounded-md bg-primary px-3 py-1.5 text-xs text-primary-foreground hover:bg-primary/90"
								>
									<Plus class="h-3.5 w-3.5" />
									Add Stage
								</button>
							</div>
							{#if apiStages.length === 0}
								<p class="text-center text-sm text-muted-foreground py-4">No stages configured</p>
							{:else}
								<div class="space-y-2">
									{#each apiStages as stage (stage.StageName)}
										<div class="flex items-center justify-between rounded-md border bg-background p-3">
											<div>
												<p class="text-sm font-medium">{stage.StageName}</p>
												{#if stage.Description}
													<p class="text-xs text-muted-foreground">{stage.Description}</p>
												{/if}
												<div class="mt-1 flex gap-2">
													{#if stage.AutoDeploy}
														<span class="rounded-full bg-green-100 px-2 py-0.5 text-xs text-green-700 dark:bg-green-900/30 dark:text-green-300">Auto-deploy</span>
													{/if}
													{#if stage.DeploymentId}
														<span class="rounded-full bg-blue-100 px-2 py-0.5 text-xs text-blue-700 dark:bg-blue-900/30 dark:text-blue-300">deployed</span>
													{/if}
												</div>
											</div>
											<button
												onclick={() => deleteStage(stage)}
												class="rounded-md p-1.5 text-muted-foreground hover:bg-destructive/10 hover:text-destructive"
											>
												<Trash2 class="h-4 w-4" />
											</button>
										</div>
									{/each}
								</div>
							{/if}
						{/if}

						<!-- Integrations Tab -->
						{#if apiDetailTab === 'integrations'}
							<div class="mb-3 flex justify-end">
								<button
									onclick={() => (showCreateIntegModal = true)}
									class="flex items-center gap-1.5 rounded-md bg-primary px-3 py-1.5 text-xs text-primary-foreground hover:bg-primary/90"
								>
									<Plus class="h-3.5 w-3.5" />
									Add Integration
								</button>
							</div>
							{#if apiIntegrations.length === 0}
								<p class="text-center text-sm text-muted-foreground py-4">No integrations configured</p>
							{:else}
								<div class="space-y-2">
									{#each apiIntegrations as integ (integ.IntegrationId)}
										<div class="flex items-center justify-between rounded-md border bg-background p-3">
											<div>
												<div class="flex items-center gap-2">
													<span class="rounded-full bg-purple-100 px-2 py-0.5 text-xs font-semibold text-purple-700 dark:bg-purple-900/30 dark:text-purple-300">
														{integ.IntegrationType}
													</span>
													{#if integ.PayloadFormatVersion}
														<span class="text-xs text-muted-foreground">v{integ.PayloadFormatVersion}</span>
													{/if}
												</div>
												{#if integ.IntegrationUri}
													<p class="mt-1 text-xs font-mono text-muted-foreground truncate max-w-xs">{integ.IntegrationUri}</p>
												{/if}
												{#if integ.TimeoutInMillis}
													<p class="text-xs text-muted-foreground">timeout: {integ.TimeoutInMillis}ms</p>
												{/if}
											</div>
											<button
												onclick={() => deleteIntegration(integ)}
												class="rounded-md p-1.5 text-muted-foreground hover:bg-destructive/10 hover:text-destructive"
											>
												<Trash2 class="h-4 w-4" />
											</button>
										</div>
									{/each}
								</div>
							{/if}
						{/if}

						<!-- Authorizers Tab -->
						{#if apiDetailTab === 'authorizers'}
							<div class="mb-3 flex justify-end">
								<button
									onclick={() => (showCreateAuthModal = true)}
									class="flex items-center gap-1.5 rounded-md bg-primary px-3 py-1.5 text-xs text-primary-foreground hover:bg-primary/90"
								>
									<Plus class="h-3.5 w-3.5" />
									Add Authorizer
								</button>
							</div>
							{#if apiAuthorizers.length === 0}
								<p class="text-center text-sm text-muted-foreground py-4">No authorizers configured</p>
							{:else}
								<div class="space-y-2">
									{#each apiAuthorizers as auth (auth.AuthorizerId)}
										<div class="flex items-center justify-between rounded-md border bg-background p-3">
											<div>
												<div class="flex items-center gap-2">
													<p class="text-sm font-medium">{auth.Name}</p>
													<span class="rounded-full bg-amber-100 px-2 py-0.5 text-xs font-semibold text-amber-700 dark:bg-amber-900/30 dark:text-amber-300">
														{auth.AuthorizerType}
													</span>
												</div>
												{#if auth.IdentitySource?.length}
													<p class="text-xs text-muted-foreground">{auth.IdentitySource.join(', ')}</p>
												{/if}
												{#if auth.JwtConfiguration?.Issuer}
													<p class="text-xs text-muted-foreground truncate max-w-xs">issuer: {auth.JwtConfiguration.Issuer}</p>
												{/if}
											</div>
											<button
												onclick={() => deleteAuthorizer(auth)}
												class="rounded-md p-1.5 text-muted-foreground hover:bg-destructive/10 hover:text-destructive"
											>
												<Trash2 class="h-4 w-4" />
											</button>
										</div>
									{/each}
								</div>
							{/if}
						{/if}

						<!-- Deployments Tab -->
						{#if apiDetailTab === 'deployments'}
							{#if apiDeployments.length === 0}
								<p class="text-center text-sm text-muted-foreground py-4">No deployments yet — use the Deploy button above</p>
							{:else}
								<div class="space-y-2">
									{#each apiDeployments as depl (depl.DeploymentId)}
										<div class="flex items-center justify-between rounded-md border bg-background p-3">
											<div>
												<p class="text-sm font-mono">{depl.DeploymentId}</p>
												{#if depl.Description}
													<p class="text-xs text-muted-foreground">{depl.Description}</p>
												{/if}
												<span class="mt-1 inline-block rounded-full bg-green-100 px-2 py-0.5 text-xs text-green-700 dark:bg-green-900/30 dark:text-green-300">
													{depl.DeploymentStatus}
												</span>
											</div>
											<button
												onclick={() => deleteDeployment(depl)}
												class="rounded-md p-1.5 text-muted-foreground hover:bg-destructive/10 hover:text-destructive"
											>
												<Trash2 class="h-4 w-4" />
											</button>
										</div>
									{/each}
								</div>
							{/if}
						{/if}
					</div>
				{/if}
			</div>
		{/if}
	{/if}

	<!-- ═══════════════════════════ Domain Names Tab ══════════════════════════════ -->
	{#if topTab === 'domainnames'}
		<div class="flex items-center justify-between gap-4">
			<div class="relative flex-1">
				<Search class="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
				<input type="text" placeholder="Search domains…" bind:value={searchQuery}
					class="w-full rounded-md border bg-background pl-9 pr-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
			</div>
			<button onclick={() => (showCreateDomainModal = true)}
				class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90">
				<Plus class="h-4 w-4" />Add Domain
			</button>
		</div>
		{#if filteredDomains.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-center">
				<Link class="mb-3 h-12 w-12 text-muted-foreground/40" />
				<p class="text-muted-foreground">No custom domain names</p>
			</div>
		{:else}
			<div class="space-y-2">
				{#each filteredDomains as dn (dn.DomainName)}
					<div class="flex items-center justify-between rounded-lg border bg-card p-4">
						<div class="flex items-center gap-3">
							<Link class="h-5 w-5 text-blue-500 flex-shrink-0" />
							<div>
								<p class="font-medium">{dn.DomainName}</p>
								{#each dn.DomainNameConfigurations ?? [] as cfg}
									<p class="text-xs text-muted-foreground">{cfg.EndpointType ?? ''} · {cfg.DomainNameStatus ?? ''}</p>
								{/each}
							</div>
						</div>
						<button onclick={() => deleteDomainName(dn)}
							class="rounded-md p-1.5 text-muted-foreground hover:bg-destructive/10 hover:text-destructive">
							<Trash2 class="h-4 w-4" />
						</button>
					</div>
				{/each}
			</div>
		{/if}
	{/if}

	<!-- ═══════════════════════════ VPC Links Tab ══════════════════════════════ -->
	{#if topTab === 'vpclinkstab'}
		<div class="flex items-center justify-between gap-4">
			<div class="relative flex-1">
				<Search class="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
				<input type="text" placeholder="Search VPC links…" bind:value={searchQuery}
					class="w-full rounded-md border bg-background pl-9 pr-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
			</div>
			<button onclick={() => (showCreateVpcLinkModal = true)}
				class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90">
				<Plus class="h-4 w-4" />Create VPC Link
			</button>
		</div>
		{#if filteredVpcLinks.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-center">
				<Network class="mb-3 h-12 w-12 text-muted-foreground/40" />
				<p class="text-muted-foreground">No VPC links</p>
			</div>
		{:else}
			<div class="space-y-2">
				{#each filteredVpcLinks as vl (vl.VpcLinkId)}
					<div class="flex items-center justify-between rounded-lg border bg-card p-4">
						<div class="flex items-center gap-3">
							<Network class="h-5 w-5 text-green-500 flex-shrink-0" />
							<div>
								<p class="font-medium">{vl.Name}</p>
								<p class="text-xs text-muted-foreground font-mono">{vl.VpcLinkId}</p>
								<div class="flex gap-2 mt-1">
									<span class="rounded-full bg-green-100 px-2 py-0.5 text-xs text-green-700 dark:bg-green-900/30 dark:text-green-300">
										{vl.VpcLinkStatus}
									</span>
									{#if vl.SubnetIds?.length}
										<span class="text-xs text-muted-foreground">{vl.SubnetIds.length} subnets</span>
									{/if}
								</div>
							</div>
						</div>
						<button onclick={() => deleteVpcLink(vl)}
							class="rounded-md p-1.5 text-muted-foreground hover:bg-destructive/10 hover:text-destructive">
							<Trash2 class="h-4 w-4" />
						</button>
					</div>
				{/each}
			</div>
		{/if}
	{/if}

	<!-- ═══════════════════════════ Metrics Tab ═══════════════════════════════════ -->
	{#if topTab === 'metrics'}
		<div class="grid grid-cols-2 gap-4 sm:grid-cols-4">
			{#each [
				{ label: 'Total APIs', value: metrics.totalApis, color: 'text-purple-600' },
				{ label: 'HTTP APIs', value: metrics.httpApis, color: 'text-blue-600' },
				{ label: 'WebSocket APIs', value: metrics.wsApis, color: 'text-green-600' },
				{ label: 'Routes', value: metrics.totalRoutes, color: 'text-amber-600' },
				{ label: 'Integrations', value: metrics.totalIntegrations, color: 'text-pink-600' },
				{ label: 'Stages', value: metrics.totalStages, color: 'text-indigo-600' },
				{ label: 'Domain Names', value: metrics.totalDomains, color: 'text-cyan-600' },
				{ label: 'VPC Links', value: metrics.totalVpcLinks, color: 'text-orange-600' },
			] as m (m.label)}
				<div class="rounded-lg border bg-card p-4">
					<p class="text-sm text-muted-foreground">{m.label}</p>
					<p class="text-3xl font-bold {m.color}">{m.value}</p>
				</div>
			{/each}
		</div>
	{/if}

	<!-- ═══════════════════════════ Docs Tab ══════════════════════════════════════ -->
	{#if topTab === 'docs'}
		<div class="rounded-lg border bg-card p-6">
			<h2 class="mb-4 text-lg font-semibold">Supported API Gateway V2 Operations</h2>
			<div class="grid grid-cols-1 gap-1 sm:grid-cols-2 lg:grid-cols-3">
				{#each supportedOperations as op (op)}
					<div class="flex items-center gap-2 rounded-md px-2 py-1.5 text-sm hover:bg-accent">
						<span class="h-1.5 w-1.5 rounded-full bg-green-500 flex-shrink-0"></span>
						<span class="font-mono text-xs">{op}</span>
					</div>
				{/each}
			</div>
		</div>
	{/if}
</div>

<!-- ═══════════════════════════════ Modals ══════════════════════════════════════ -->

<!-- Create API Modal -->
{#if showCreateApiModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg border bg-card p-6 shadow-lg">
			<h2 class="mb-4 text-lg font-semibold">Create HTTP/WebSocket API</h2>
			<div class="space-y-4">
				<div>
					<label class="mb-1 block text-sm font-medium" for="new-api-name">Name <span class="text-destructive">*</span></label>
					<input id="new-api-name" bind:value={newApiName} placeholder="my-api" class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
				<div>
					<label class="mb-1 block text-sm font-medium" for="new-api-protocol">Protocol</label>
					<select id="new-api-protocol" bind:value={newApiProtocol} class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary">
						<option value="HTTP">HTTP</option>
						<option value="WEBSOCKET">WEBSOCKET</option>
					</select>
				</div>
				<div>
					<label class="mb-1 block text-sm font-medium" for="new-api-description">Description</label>
					<input id="new-api-description" bind:value={newApiDescription} placeholder="Optional description" class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
			</div>
			<div class="mt-6 flex justify-end gap-3">
				<button onclick={() => (showCreateApiModal = false)} class="rounded-md border px-4 py-2 text-sm hover:bg-accent">Cancel</button>
				<button onclick={createApi} disabled={creating || !newApiName.trim()} class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50">
					{creating ? 'Creating…' : 'Create'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Create Route Modal -->
{#if showCreateRouteModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg border bg-card p-6 shadow-lg">
			<h2 class="mb-4 text-lg font-semibold">Add Route</h2>
			<div class="space-y-4">
				<div>
					<label class="mb-1 block text-sm font-medium" for="new-route-key">Route Key <span class="text-destructive">*</span></label>
					<input id="new-route-key" bind:value={newRouteKey} placeholder="GET /items or $connect" class="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
				<div>
					<label class="mb-1 block text-sm font-medium" for="new-route-target">Target (optional)</label>
					<input id="new-route-target" bind:value={newRouteTarget} placeholder="integrations/&#123;integrationId&#125;" class="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
			</div>
			<div class="mt-6 flex justify-end gap-3">
				<button onclick={() => (showCreateRouteModal = false)} class="rounded-md border px-4 py-2 text-sm hover:bg-accent">Cancel</button>
				<button onclick={createRoute} disabled={creating || !newRouteKey.trim()} class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50">
					{creating ? 'Creating…' : 'Create'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Create Stage Modal -->
{#if showCreateStageModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg border bg-card p-6 shadow-lg">
			<h2 class="mb-4 text-lg font-semibold">Add Stage</h2>
			<div class="space-y-4">
				<div>
					<label class="mb-1 block text-sm font-medium" for="new-stage-name">Stage Name <span class="text-destructive">*</span></label>
					<input id="new-stage-name" bind:value={newStageName} placeholder="prod or $default" class="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
				<div>
					<label class="mb-1 block text-sm font-medium" for="new-stage-description">Description</label>
					<input id="new-stage-description" bind:value={newStageDescription} placeholder="Optional description" class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
				<div class="flex items-center gap-2">
					<input type="checkbox" bind:checked={newStageAutoDeploy} id="autoDeploy" class="h-4 w-4" />
					<label for="autoDeploy" class="text-sm">Auto-deploy on change</label>
				</div>
			</div>
			<div class="mt-6 flex justify-end gap-3">
				<button onclick={() => (showCreateStageModal = false)} class="rounded-md border px-4 py-2 text-sm hover:bg-accent">Cancel</button>
				<button onclick={createStage} disabled={creating || !newStageName.trim()} class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50">
					{creating ? 'Creating…' : 'Create'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Create Integration Modal -->
{#if showCreateIntegModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg border bg-card p-6 shadow-lg">
			<h2 class="mb-4 text-lg font-semibold">Add Integration</h2>
			<div class="space-y-4">
				<div>
					<label class="mb-1 block text-sm font-medium" for="new-integ-type">Integration Type</label>
					<select id="new-integ-type" bind:value={newIntegType} class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary">
						<option value="AWS_PROXY">AWS_PROXY (Lambda)</option>
						<option value="HTTP_PROXY">HTTP_PROXY</option>
						<option value="HTTP">HTTP</option>
						<option value="MOCK">MOCK</option>
					</select>
				</div>
				{#if newIntegType !== 'MOCK'}
					<div>
						<label class="mb-1 block text-sm font-medium" for="new-integ-uri">Integration URI</label>
						<input id="new-integ-uri" bind:value={newIntegUri} placeholder="arn:aws:lambda:... or https://..." class="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-primary" />
					</div>
					{#if newIntegType === 'HTTP' || newIntegType === 'HTTP_PROXY'}
						<div>
							<label class="mb-1 block text-sm font-medium" for="new-integ-method">HTTP Method</label>
							<select id="new-integ-method" bind:value={newIntegMethod} class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary">
								{#each ['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'ANY'] as m (m)}
									<option>{m}</option>
								{/each}
							</select>
						</div>
					{/if}
				{/if}
			</div>
			<div class="mt-6 flex justify-end gap-3">
				<button onclick={() => (showCreateIntegModal = false)} class="rounded-md border px-4 py-2 text-sm hover:bg-accent">Cancel</button>
				<button onclick={createIntegration} disabled={creating} class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50">
					{creating ? 'Creating…' : 'Create'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Create Authorizer Modal -->
{#if showCreateAuthModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg border bg-card p-6 shadow-lg">
			<h2 class="mb-4 text-lg font-semibold">Add Authorizer</h2>
			<div class="space-y-4">
				<div>
					<label class="mb-1 block text-sm font-medium" for="new-auth-name">Name <span class="text-destructive">*</span></label>
					<input id="new-auth-name" bind:value={newAuthName} placeholder="my-jwt-authorizer" class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
				<div>
					<label class="mb-1 block text-sm font-medium" for="new-auth-type">Type</label>
					<select id="new-auth-type" bind:value={newAuthType} class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary">
						<option value="JWT">JWT</option>
						<option value="REQUEST">REQUEST (Lambda)</option>
					</select>
				</div>
				<div>
					<label class="mb-1 block text-sm font-medium" for="new-auth-identity-source">Identity Source</label>
					<input id="new-auth-identity-source" bind:value={newAuthIdentitySource} class="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
				{#if newAuthType === 'JWT'}
					<div>
						<label class="mb-1 block text-sm font-medium" for="new-auth-issuer">JWT Issuer</label>
						<input id="new-auth-issuer" bind:value={newAuthIssuer} placeholder="https://cognito-idp.us-east-1.amazonaws.com/..." class="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-primary" />
					</div>
					<div>
						<label class="mb-1 block text-sm font-medium" for="new-auth-audience">Audience</label>
						<input id="new-auth-audience" bind:value={newAuthAudience} placeholder="my-client-id" class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
					</div>
				{/if}
			</div>
			<div class="mt-6 flex justify-end gap-3">
				<button onclick={() => (showCreateAuthModal = false)} class="rounded-md border px-4 py-2 text-sm hover:bg-accent">Cancel</button>
				<button onclick={createAuthorizer} disabled={creating || !newAuthName.trim()} class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50">
					{creating ? 'Creating…' : 'Create'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Deploy Modal -->
{#if showDeployModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg border bg-card p-6 shadow-lg">
			<h2 class="mb-4 text-lg font-semibold">Deploy API</h2>
			<div class="space-y-4">
				<div>
					<label class="mb-1 block text-sm font-medium" for="deploy-stage-name">Stage Name</label>
					<input id="deploy-stage-name" bind:value={deployStageName} placeholder="$default or prod" class="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-primary" />
					<p class="mt-1 text-xs text-muted-foreground">Leave as $default for HTTP APIs or choose an existing stage</p>
				</div>
				<div>
					<label class="mb-1 block text-sm font-medium" for="deploy-description">Description</label>
					<input id="deploy-description" bind:value={deployDescription} placeholder="Optional deployment description" class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
			</div>
			<div class="mt-6 flex justify-end gap-3">
				<button onclick={() => (showDeployModal = false)} class="rounded-md border px-4 py-2 text-sm hover:bg-accent">Cancel</button>
				<button onclick={deployApi} disabled={deploying} class="rounded-md bg-green-600 px-4 py-2 text-sm text-white hover:bg-green-700 disabled:opacity-50">
					{deploying ? 'Deploying…' : 'Deploy'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Create Domain Name Modal -->
{#if showCreateDomainModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg border bg-card p-6 shadow-lg">
			<h2 class="mb-4 text-lg font-semibold">Create Custom Domain</h2>
			<div class="space-y-4">
				<div>
					<label class="mb-1 block text-sm font-medium" for="new-domain-name">Domain Name <span class="text-destructive">*</span></label>
					<input id="new-domain-name" bind:value={newDomainName} placeholder="api.example.com" class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
				<div>
					<label class="mb-1 block text-sm font-medium" for="new-domain-cert-arn">Certificate ARN (optional)</label>
					<input id="new-domain-cert-arn" bind:value={newDomainCertArn} placeholder="arn:aws:acm:..." class="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
			</div>
			<div class="mt-6 flex justify-end gap-3">
				<button onclick={() => (showCreateDomainModal = false)} class="rounded-md border px-4 py-2 text-sm hover:bg-accent">Cancel</button>
				<button onclick={createDomainName} disabled={creating || !newDomainName.trim()} class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50">
					{creating ? 'Creating…' : 'Create'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Create VPC Link Modal -->
{#if showCreateVpcLinkModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg border bg-card p-6 shadow-lg">
			<h2 class="mb-4 text-lg font-semibold">Create VPC Link</h2>
			<div class="space-y-4">
				<div>
					<label class="mb-1 block text-sm font-medium" for="new-vpc-link-name">Name <span class="text-destructive">*</span></label>
					<input id="new-vpc-link-name" bind:value={newVpcLinkName} placeholder="my-vpc-link" class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
				<div>
					<label class="mb-1 block text-sm font-medium" for="new-vpc-link-subnets">Subnet IDs (comma-separated)</label>
					<input id="new-vpc-link-subnets" bind:value={newVpcLinkSubnets} placeholder="subnet-aaa, subnet-bbb" class="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
			</div>
			<div class="mt-6 flex justify-end gap-3">
				<button onclick={() => (showCreateVpcLinkModal = false)} class="rounded-md border px-4 py-2 text-sm hover:bg-accent">Cancel</button>
				<button onclick={createVpcLink} disabled={creating || !newVpcLinkName.trim()} class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50">
					{creating ? 'Creating…' : 'Create'}
				</button>
			</div>
		</div>
	</div>
{/if}
