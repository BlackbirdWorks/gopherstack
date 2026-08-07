import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import ApiGatewayV2Page from "./+page.svelte";
import { ALL_REGIONS, DEFAULT_REGION, setStoredRegion } from "$lib/region.svelte";

function stubRegionsWithData(regions: string[]): void {
  vi.stubGlobal(
    "fetch",
    vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: () => Promise.resolve({ regions }),
    }),
  );
}

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getAPIGatewayV2Client: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

// This page's modals are plain `{#if showX}` divs, not the shared
// role="dialog" Modal component. The ~20 form <label>s in them are now wired
// to their <input> via for/id, so getByLabelText would resolve them, but
// these tests still target inputs by placeholder text to minimize the diff.
const exampleApi = {
  ApiId: "api-1",
  Name: "my-api",
  ProtocolType: "HTTP",
  Description: "My HTTP API",
};

async function renderWithApis(apis: (typeof exampleApi)[] = [exampleApi]) {
  // GetApisCommand
  mockSend.mockResolvedValueOnce({ Items: apis });
  render(ApiGatewayV2Page);
  if (apis.length > 0) {
    await waitFor(() => screen.getByText(apis[0].Name));
  } else {
    await waitFor(() => screen.getByText("No HTTP APIs found"));
  }
}

/** Renders with one API loaded and selects it, loading empty detail tabs. */
async function renderWithSelectedApi() {
  await renderWithApis();
  // GetRoutesCommand
  mockSend.mockResolvedValueOnce({ Items: [] });
  // GetStagesCommand
  mockSend.mockResolvedValueOnce({ Items: [] });
  // GetIntegrationsCommand
  mockSend.mockResolvedValueOnce({ Items: [] });
  // GetAuthorizersCommand
  mockSend.mockResolvedValueOnce({ Items: [] });
  // GetDeploymentsCommand
  mockSend.mockResolvedValueOnce({ Items: [] });
  await fireEvent.click(screen.getByText(exampleApi.Name));
  await waitFor(() => screen.getByText("No routes configured"));
}

function apiRow(name: string) {
  const el = screen.getByText(name).closest(".rounded-lg");
  if (!el) throw new Error(`row for ${name} not found`);
  return el as HTMLElement;
}

/** Row container for the smaller detail-tab list items (routes/stages/etc). */
function detailRow(text: string) {
  const el = screen.getByText(text).closest(".rounded-md");
  if (!el) throw new Error(`row for ${text} not found`);
  return el as HTMLElement;
}

describe("ApiGatewayV2 Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
    setStoredRegion(DEFAULT_REGION);
  });

  it("renders the page title and top tabs", () => {
    mockSend.mockResolvedValueOnce({ Items: [] });
    render(ApiGatewayV2Page);
    expect(screen.getByText("API Gateway V2")).toBeInTheDocument();
    expect(screen.getByText("HTTP APIs")).toBeInTheDocument();
    expect(screen.getByText("Domain Names")).toBeInTheDocument();
    expect(screen.getByText("VPC Links")).toBeInTheDocument();
  });

  it("loads APIs on mount, rendering the protocol-type badge and description", async () => {
    await renderWithApis();
    expect(mockSend).toHaveBeenCalledWith(expect.objectContaining({ input: {} }));
    expect(screen.getByText("HTTP")).toBeInTheDocument();
    expect(screen.getByText(exampleApi.Description)).toBeInTheDocument();
  });

  it("shows a toast error including the AWS error code when loading APIs fails", async () => {
    const error = Object.assign(new Error("Rate exceeded."), {
      name: "ThrottlingException",
    });
    mockSend.mockRejectedValueOnce(error);
    const { toast } = await import("svelte-sonner");

    render(ApiGatewayV2Page);

    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith(
        "Failed to load APIs: ThrottlingException: Rate exceeded.",
      );
    });
  });

  it("creates an HTTP API via the modal", async () => {
    await renderWithApis([]);

    await fireEvent.click(screen.getByRole("button", { name: "Create API" }));
    await fireEvent.input(screen.getByPlaceholderText("my-api"), {
      target: { value: "new-api" },
    });
    await fireEvent.input(screen.getByPlaceholderText("Optional description"), {
      target: { value: "A new API" },
    });

    // CreateApiCommand
    mockSend.mockResolvedValueOnce({ ApiId: "api-2" });
    mockSend.mockResolvedValueOnce({ Items: [{ ...exampleApi, ApiId: "api-2", Name: "new-api" }] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByText("new-api")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      // call 1 is always the initial GetApisCommand from mount
      2,
      expect.objectContaining({
        input: {
          Name: "new-api",
          Description: "A new API",
          ProtocolType: "HTTP",
        },
      }),
    );
  });

  it("deletes an API after confirming, sending only the ApiId", async () => {
    await renderWithApis();

    const deleteBtn = within(apiRow(exampleApi.Name)).getAllByRole("button").at(-1)!;
    // DeleteApiCommand
    mockSend.mockResolvedValueOnce({});
    // reload
    mockSend.mockResolvedValueOnce({ Items: [] });

    await fireEvent.click(deleteBtn);

    expect(confirmDestructive).toHaveBeenCalledWith(
      expect.objectContaining({ message: expect.stringContaining(exampleApi.Name) }),
    );
    expect(mockSend).toHaveBeenNthCalledWith(
      // call 1 is always the initial GetApisCommand from mount
      2,
      expect.objectContaining({ input: { ApiId: exampleApi.ApiId } }),
    );
    await waitFor(() => {
      expect(screen.getByText("No HTTP APIs found")).toBeInTheDocument();
    });
  });

  it("does not delete an API when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    await renderWithApis();
    const callsBefore = mockSend.mock.calls.length;

    const deleteBtn = within(apiRow(exampleApi.Name)).getAllByRole("button").at(-1)!;
    await fireEvent.click(deleteBtn);

    expect(confirmDestructive).toHaveBeenCalled();
    expect(mockSend).toHaveBeenCalledTimes(callsBefore);
    expect(screen.getByText(exampleApi.Name)).toBeInTheDocument();
  });

  it("selects an API and loads its detail tabs (routes/stages/integrations/authorizers/deployments) scoped by ApiId", async () => {
    await renderWithSelectedApi();

    for (const [nth, expectedType] of [
      [1, "GetRoutesCommand"],
      [2, "GetStagesCommand"],
      [3, "GetIntegrationsCommand"],
      [4, "GetAuthorizersCommand"],
      [5, "GetDeploymentsCommand"],
    ] as const) {
      // call 0 was the initial GetApisCommand
      const call = mockSend.mock.calls[nth];
      expect(call[0].constructor.name).toBe(expectedType);
      expect(call[0].input).toEqual({ ApiId: exampleApi.ApiId });
    }
  });

  it("creates a route on the selected API and renders its route key", async () => {
    await renderWithSelectedApi();

    await fireEvent.click(screen.getByRole("button", { name: "Add Route" }));
    await fireEvent.input(screen.getByPlaceholderText("GET /items or $connect"), {
      target: { value: "POST /orders" },
    });

    const newRoute = { RouteId: "route-1", RouteKey: "POST /orders" };
    // CreateRouteCommand
    mockSend.mockResolvedValueOnce({});
    // GetRoutesCommand
    mockSend.mockResolvedValueOnce({ Items: [newRoute] });
    // GetStagesCommand
    mockSend.mockResolvedValueOnce({ Items: [] });
    // GetIntegrationsCommand
    mockSend.mockResolvedValueOnce({ Items: [] });
    // GetAuthorizersCommand
    mockSend.mockResolvedValueOnce({ Items: [] });
    // GetDeploymentsCommand
    mockSend.mockResolvedValueOnce({ Items: [] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({
        input: { ApiId: exampleApi.ApiId, RouteKey: "POST /orders" },
      }),
    );
    await waitFor(() => {
      expect(screen.getByText("POST /orders")).toBeInTheDocument();
    });
  });

  it("deletes a route after confirming", async () => {
    await renderWithApis();
    const route = { RouteId: "route-1", RouteKey: "GET /items" };
    // GetRoutesCommand
    mockSend.mockResolvedValueOnce({ Items: [route] });
    // GetStagesCommand
    mockSend.mockResolvedValueOnce({ Items: [] });
    // GetIntegrationsCommand
    mockSend.mockResolvedValueOnce({ Items: [] });
    // GetAuthorizersCommand
    mockSend.mockResolvedValueOnce({ Items: [] });
    // GetDeploymentsCommand
    mockSend.mockResolvedValueOnce({ Items: [] });
    await fireEvent.click(screen.getByText(exampleApi.Name));
    await waitFor(() => screen.getByText(route.RouteKey));

    // DeleteRouteCommand
    mockSend.mockResolvedValueOnce({});
    // reload routes
    mockSend.mockResolvedValueOnce({ Items: [] });
    mockSend.mockResolvedValueOnce({ Items: [] });
    mockSend.mockResolvedValueOnce({ Items: [] });
    mockSend.mockResolvedValueOnce({ Items: [] });
    mockSend.mockResolvedValueOnce({ Items: [] });

    await fireEvent.click(within(detailRow(route.RouteKey)).getByRole("button"));

    expect(confirmDestructive).toHaveBeenCalledWith(
      expect.objectContaining({ message: expect.stringContaining("GET /items") }),
    );
    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({
        input: { ApiId: exampleApi.ApiId, RouteId: route.RouteId },
      }),
    );
    await waitFor(() => {
      expect(screen.getByText("No routes configured")).toBeInTheDocument();
    });
  });

  it("creates a stage with auto-deploy checked", async () => {
    await renderWithSelectedApi();

    await fireEvent.click(screen.getByText("Stages"));
    await fireEvent.click(screen.getByRole("button", { name: "Add Stage" }));
    await fireEvent.input(screen.getByPlaceholderText("prod or $default"), {
      target: { value: "prod" },
    });
    await fireEvent.click(screen.getByRole("checkbox"));

    const newStage = { StageName: "prod", AutoDeploy: true };
    // CreateStageCommand
    mockSend.mockResolvedValueOnce({});
    // routes
    mockSend.mockResolvedValueOnce({ Items: [] });
    // stages
    mockSend.mockResolvedValueOnce({ Items: [newStage] });
    // integrations
    mockSend.mockResolvedValueOnce({ Items: [] });
    // authorizers
    mockSend.mockResolvedValueOnce({ Items: [] });
    // deployments
    mockSend.mockResolvedValueOnce({ Items: [] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({
        input: { ApiId: exampleApi.ApiId, StageName: "prod", AutoDeploy: true },
      }),
    );
    await waitFor(() => {
      expect(screen.getByText("Auto-deploy")).toBeInTheDocument();
    });
  });

  it("creates an AWS_PROXY integration and renders its type badge", async () => {
    await renderWithSelectedApi();

    await fireEvent.click(screen.getByText("Integrations"));
    await fireEvent.click(screen.getByRole("button", { name: "Add Integration" }));
    await fireEvent.input(screen.getByPlaceholderText("arn:aws:lambda:... or https://..."), {
      target: { value: "arn:aws:lambda:us-east-1:123456789012:function:handler" },
    });

    const newIntegration = {
      IntegrationId: "integ-1",
      IntegrationType: "AWS_PROXY",
      IntegrationUri: "arn:aws:lambda:us-east-1:123456789012:function:handler",
    };
    // CreateIntegrationCommand
    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ Items: [] });
    mockSend.mockResolvedValueOnce({ Items: [] });
    mockSend.mockResolvedValueOnce({ Items: [newIntegration] });
    mockSend.mockResolvedValueOnce({ Items: [] });
    mockSend.mockResolvedValueOnce({ Items: [] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({
        input: {
          ApiId: exampleApi.ApiId,
          IntegrationType: "AWS_PROXY",
          IntegrationUri: "arn:aws:lambda:us-east-1:123456789012:function:handler",
          IntegrationMethod: "POST",
        },
      }),
    );
    await waitFor(() => {
      expect(screen.getByText("AWS_PROXY")).toBeInTheDocument();
    });
  });

  it("creates a JWT authorizer with issuer and audience", async () => {
    await renderWithSelectedApi();

    await fireEvent.click(screen.getByText("Authorizers"));
    await fireEvent.click(screen.getByRole("button", { name: "Add Authorizer" }));
    await fireEvent.input(screen.getByPlaceholderText("my-jwt-authorizer"), {
      target: { value: "cognito-jwt" },
    });
    await fireEvent.input(
      screen.getByPlaceholderText("https://cognito-idp.us-east-1.amazonaws.com/..."),
      { target: { value: "https://issuer.example.com" } },
    );
    await fireEvent.input(screen.getByPlaceholderText("my-client-id"), {
      target: { value: "client-abc" },
    });

    const newAuth = { AuthorizerId: "auth-1", Name: "cognito-jwt", AuthorizerType: "JWT" };
    // CreateAuthorizerCommand
    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ Items: [] });
    mockSend.mockResolvedValueOnce({ Items: [] });
    mockSend.mockResolvedValueOnce({ Items: [] });
    mockSend.mockResolvedValueOnce({ Items: [newAuth] });
    mockSend.mockResolvedValueOnce({ Items: [] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({
        input: {
          ApiId: exampleApi.ApiId,
          Name: "cognito-jwt",
          AuthorizerType: "JWT",
          IdentitySource: ["$request.header.Authorization"],
          JwtConfiguration: {
            Issuer: "https://issuer.example.com",
            Audience: ["client-abc"],
          },
        },
      }),
    );
    await waitFor(() => {
      expect(screen.getByText("cognito-jwt")).toBeInTheDocument();
    });
  });

  it("deploys the selected API to a stage", async () => {
    await renderWithSelectedApi();

    await fireEvent.click(screen.getByRole("button", { name: "Deploy" }));
    // The underlying page (with its own header "Deploy" button) stays in the
    // DOM behind the modal overlay, so scope the submit click to the modal
    // itself -- located by its heading, since it has no role="dialog".
    const modal = screen.getByText("Deploy API").closest("div") as HTMLElement;
    await fireEvent.input(within(modal).getByPlaceholderText("$default or prod"), {
      target: { value: "prod" },
    });

    const newDeployment = { DeploymentId: "dep-1", DeploymentStatus: "DEPLOYED" };
    // CreateDeploymentCommand
    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ Items: [] });
    mockSend.mockResolvedValueOnce({ Items: [] });
    mockSend.mockResolvedValueOnce({ Items: [] });
    mockSend.mockResolvedValueOnce({ Items: [] });
    mockSend.mockResolvedValueOnce({ Items: [newDeployment] });

    await fireEvent.click(within(modal).getByRole("button", { name: "Deploy" }));

    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({
        input: { ApiId: exampleApi.ApiId, StageName: "prod", Description: undefined },
      }),
    );

    await fireEvent.click(screen.getByText("Deployments"));
    await waitFor(() => {
      expect(screen.getByText("dep-1")).toBeInTheDocument();
    });
    expect(screen.getByText("DEPLOYED")).toBeInTheDocument();
  });

  it("creates and deletes a custom domain on the Domain Names tab", async () => {
    // initial GetApisCommand
    mockSend.mockResolvedValueOnce({ Items: [] });
    render(ApiGatewayV2Page);
    await waitFor(() => screen.getByText("No HTTP APIs found"));

    // GetDomainNamesCommand
    mockSend.mockResolvedValueOnce({ Items: [] });
    await fireEvent.click(screen.getByText("Domain Names"));
    await waitFor(() => screen.getByText("No custom domain names"));

    await fireEvent.click(screen.getByRole("button", { name: "Add Domain" }));
    await fireEvent.input(screen.getByPlaceholderText("api.example.com"), {
      target: { value: "api.mysite.com" },
    });
    await fireEvent.input(screen.getByPlaceholderText("arn:aws:acm:..."), {
      target: { value: "arn:aws:acm:us-east-1:123456789012:certificate/abc" },
    });

    const domain = {
      DomainName: "api.mysite.com",
      DomainNameConfigurations: [{ EndpointType: "REGIONAL", DomainNameStatus: "AVAILABLE" }],
    };
    // CreateDomainNameCommand
    mockSend.mockResolvedValueOnce({});
    // reload
    mockSend.mockResolvedValueOnce({ Items: [domain] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({
        input: {
          DomainName: "api.mysite.com",
          DomainNameConfigurations: [
            {
              CertificateArn: "arn:aws:acm:us-east-1:123456789012:certificate/abc",
              EndpointType: "REGIONAL",
            },
          ],
        },
      }),
    );
    await waitFor(() => {
      expect(screen.getByText("api.mysite.com")).toBeInTheDocument();
    });

    // DeleteDomainNameCommand
    mockSend.mockResolvedValueOnce({});
    // reload
    mockSend.mockResolvedValueOnce({ Items: [] });

    await fireEvent.click(within(apiRow("api.mysite.com")).getByRole("button"));

    expect(confirmDestructive).toHaveBeenCalledWith(
      expect.objectContaining({ message: expect.stringContaining("api.mysite.com") }),
    );
    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({ input: { DomainName: "api.mysite.com" } }),
    );
    await waitFor(() => {
      expect(screen.getByText("No custom domain names")).toBeInTheDocument();
    });
  });

  it("creates a VPC link, falling back to a default subnet when none are given", async () => {
    // initial GetApisCommand
    mockSend.mockResolvedValueOnce({ Items: [] });
    render(ApiGatewayV2Page);
    await waitFor(() => screen.getByText("No HTTP APIs found"));

    // GetVpcLinksCommand
    mockSend.mockResolvedValueOnce({ Items: [] });
    await fireEvent.click(screen.getByText("VPC Links"));
    await waitFor(() => screen.getByText("No VPC links"));

    await fireEvent.click(screen.getByRole("button", { name: "Create VPC Link" }));
    await fireEvent.input(screen.getByPlaceholderText("my-vpc-link"), {
      target: { value: "backend-link" },
    });
    // Subnet IDs left blank -- the page falls back to ['subnet-default'].

    const vpcLink = {
      VpcLinkId: "vpclink-1",
      Name: "backend-link",
      VpcLinkStatus: "AVAILABLE",
      SubnetIds: ["subnet-default"],
    };
    // CreateVpcLinkCommand
    mockSend.mockResolvedValueOnce({});
    // reload
    mockSend.mockResolvedValueOnce({ Items: [vpcLink] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({
        input: { Name: "backend-link", SubnetIds: ["subnet-default"] },
      }),
    );
    await waitFor(() => {
      expect(screen.getByText("backend-link")).toBeInTheDocument();
    });
    expect(screen.getByText("1 subnets")).toBeInTheDocument();
  });

  it("filters the API list via the search box", async () => {
    const secondApi = { ApiId: "api-2", Name: "other-api", ProtocolType: "WEBSOCKET" };
    mockSend.mockResolvedValueOnce({ Items: [exampleApi, secondApi] });
    render(ApiGatewayV2Page);
    await waitFor(() => screen.getByText(exampleApi.Name));
    expect(screen.getByText(secondApi.Name)).toBeInTheDocument();

    await fireEvent.input(screen.getByPlaceholderText("Search APIs…"), {
      target: { value: "other" },
    });

    expect(screen.queryByText(exampleApi.Name)).not.toBeInTheDocument();
    expect(screen.getByText(secondApi.Name)).toBeInTheDocument();
  });

  it("shows computed totals on the Metrics tab", async () => {
    await renderWithSelectedApi();
    await fireEvent.click(screen.getByText("Metrics"));

    expect(screen.getByText("Total APIs")).toBeInTheDocument();
    const totalApisCard = screen.getByText("Total APIs").parentElement;
    expect(within(totalApisCard!).getByText("1")).toBeInTheDocument();
    // "HTTP APIs" also names the always-rendered top-level tab button --
    // the metric tile renders its label in a <p>, the tab button doesn't.
    const httpApisCard = screen.getByText("HTTP APIs", { selector: "p" }).parentElement;
    expect(within(httpApisCard!).getByText("1")).toBeInTheDocument();
    const routesCard = screen.getByText("Routes", { selector: "p" }).parentElement;
    expect(within(routesCard!).getByText("0")).toBeInTheDocument();
  });

  it("lists supported operations on the Docs tab", async () => {
    mockSend.mockResolvedValueOnce({ Items: [] });
    render(ApiGatewayV2Page);
    await fireEvent.click(screen.getByText("Docs"));
    expect(screen.getByText("Supported API Gateway V2 Operations")).toBeInTheDocument();
    expect(screen.getByText("CreateApi")).toBeInTheDocument();
    expect(screen.getByText("ResetAuthorizersCache")).toBeInTheDocument();
  });

  describe("All regions mode", () => {
    it("fans GetApis out across every region with data and tags each row", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend.mockResolvedValueOnce({ Items: [exampleApi] });
      mockSend.mockResolvedValueOnce({
        Items: [{ ...exampleApi, ApiId: "api-eu", Name: "eu-api" }],
      });

      render(ApiGatewayV2Page);

      await waitFor(() => expect(screen.getByText("my-api")).toBeInTheDocument());
      expect(screen.getByText("eu-api")).toBeInTheDocument();

      vi.unstubAllGlobals();
    });

    it("issues exactly one GetApis call in single-region mode", async () => {
      mockSend.mockResolvedValueOnce({ Items: [exampleApi] });
      render(ApiGatewayV2Page);
      await waitFor(() => expect(screen.getByText("my-api")).toBeInTheDocument());
      const calls = mockSend.mock.calls.filter(
        ([cmd]) => cmd?.constructor?.name === "GetApisCommand",
      );
      expect(calls).toHaveLength(1);
    });

    it("renders the same API name from two different regions as two distinct rows, each tagged with its own region", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend.mockResolvedValueOnce({ Items: [exampleApi] });
      mockSend.mockResolvedValueOnce({ Items: [exampleApi] });

      render(ApiGatewayV2Page);

      const rows = await waitFor(() => {
        const found = screen.getAllByText("my-api");
        expect(found).toHaveLength(2);
        return found;
      });
      const chips = rows.map(
        (r) =>
          within(r.closest("[role='button']") as HTMLElement).getByTestId("region-chip")
            .textContent,
      );
      expect(chips.toSorted()).toEqual(["eu-west-1", "us-east-1"]);

      vi.unstubAllGlobals();
    });
  });
});
