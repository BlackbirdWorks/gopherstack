import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import AppMeshPage from "./+page.svelte";

function openDialog(): HTMLElement {
  const dialog = document.querySelector("dialog[open]");
  if (!dialog) throw new Error("no open dialog found");
  return dialog as HTMLElement;
}

const mockSend = vi.fn();

vi.mock("$lib/aws-client", async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getAppMeshClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

const exampleMesh = {
  meshName: "mesh-1",
  resourceOwner: "123456789012",
  version: 1,
};

const exampleMeshData = {
  meshName: "mesh-1",
  spec: { egressFilter: { type: "ALLOW_ALL" } },
  metadata: { arn: "arn:aws:appmesh:us-east-1:123456789012:mesh/mesh-1", version: 1, uid: "uid-1" },
  status: { status: "ACTIVE" },
};

const exampleNode = { meshName: "mesh-1", virtualNodeName: "node-1" };

describe("App Mesh Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title", async () => {
    mockSend.mockResolvedValueOnce({ meshes: [] });
    render(AppMeshPage);
    expect(screen.getByText("AWS App Mesh")).toBeInTheDocument();
    await waitFor(() => screen.getByText("No meshes found"));
  });

  it("lists meshes", async () => {
    mockSend.mockResolvedValueOnce({ meshes: [exampleMesh] });
    render(AppMeshPage);
    await waitFor(() => {
      expect(screen.getByText("mesh-1")).toBeInTheDocument();
    });
  });

  it("shows empty state when no meshes", async () => {
    mockSend.mockResolvedValueOnce({ meshes: [] });
    render(AppMeshPage);
    await waitFor(() => {
      expect(screen.getByText("No meshes found")).toBeInTheDocument();
    });
  });

  it("creates a mesh via the modal with the correct input", async () => {
    mockSend.mockResolvedValueOnce({ meshes: [] });
    render(AppMeshPage);
    await waitFor(() => screen.getByText("No meshes found"));

    await fireEvent.click(screen.getByText("Create mesh"));
    expect(screen.getByText("Create Mesh")).toBeInTheDocument();

    await fireEvent.input(within(openDialog()).getByLabelText("Mesh name"), {
      target: { value: "mesh-1" },
    });

    mockSend.mockResolvedValueOnce({ mesh: exampleMeshData });
    mockSend.mockResolvedValueOnce({ meshes: [exampleMesh] });

    await fireEvent.click(within(openDialog()).getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByText("mesh-1")).toBeInTheDocument();
    });

    const createCall = mockSend.mock.calls[1][0];
    expect(createCall.input).toEqual({ meshName: "mesh-1", spec: undefined });
  });

  it("deletes a mesh after confirming", async () => {
    mockSend.mockResolvedValueOnce({ meshes: [exampleMesh] });
    render(AppMeshPage);
    await waitFor(() => screen.getByText("mesh-1"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ meshes: [] });

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No meshes found")).toBeInTheDocument();
    });

    const deleteCall = mockSend.mock.calls[1][0];
    expect(deleteCall.input).toEqual({ meshName: "mesh-1" });
  });

  it("does not delete a mesh when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    mockSend.mockResolvedValueOnce({ meshes: [exampleMesh] });
    render(AppMeshPage);
    await waitFor(() => screen.getByText("mesh-1"));

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    expect(mockSend).toHaveBeenCalledTimes(1);
    expect(screen.getByText("mesh-1")).toBeInTheDocument();
  });

  it("shows the mesh detail with spec, status and ARN", async () => {
    mockSend.mockResolvedValueOnce({ meshes: [exampleMesh] });
    render(AppMeshPage);
    await waitFor(() => screen.getByText("mesh-1"));

    mockSend.mockResolvedValueOnce({ mesh: exampleMeshData });
    await fireEvent.click(screen.getByTitle("View"));

    await waitFor(() => {
      expect(within(openDialog()).getByText("ACTIVE")).toBeInTheDocument();
    });
    expect(within(openDialog()).getByText(exampleMeshData.metadata.arn)).toBeInTheDocument();
  });

  it("shows an inline error with the AWS error code when a load fails", async () => {
    const error = Object.assign(new Error("Throttled."), {
      name: "TooManyRequestsException",
      $metadata: { httpStatusCode: 429 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(AppMeshPage);

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(
        screen.getByText("TooManyRequestsException (HTTP 429): Throttled."),
      ).toBeInTheDocument();
    });
  });

  it("scopes virtual nodes to the selected mesh and creates one", async () => {
    mockSend.mockResolvedValueOnce({ meshes: [exampleMesh] });
    render(AppMeshPage);
    await waitFor(() => screen.getByText("mesh-1"));

    mockSend.mockResolvedValueOnce({ virtualNodes: [] });
    await fireEvent.click(screen.getByText("Virtual Nodes"));
    await waitFor(() => screen.getByText("No virtual nodes found"));

    const listCall = mockSend.mock.calls[1][0];
    expect(listCall.input).toEqual({ meshName: "mesh-1" });

    await fireEvent.click(screen.getByText("Create virtual node"));
    await fireEvent.input(within(openDialog()).getByLabelText("Virtual node name"), {
      target: { value: "node-1" },
    });
    await fireEvent.input(within(openDialog()).getByLabelText("Spec (JSON)"), {
      target: { value: '{"listeners": []}' },
    });

    mockSend.mockResolvedValueOnce({ virtualNode: exampleNode });
    mockSend.mockResolvedValueOnce({ virtualNodes: [exampleNode] });

    await fireEvent.click(within(openDialog()).getByRole("button", { name: "Create" }));
    await waitFor(() => screen.getByText("node-1"));

    const createCall = mockSend.mock.calls[2][0];
    expect(createCall.input).toEqual({
      meshName: "mesh-1",
      virtualNodeName: "node-1",
      spec: { listeners: [] },
    });
  });

  it("rejects invalid JSON in the virtual node spec without sending a request", async () => {
    mockSend.mockResolvedValueOnce({ meshes: [exampleMesh] });
    render(AppMeshPage);
    await waitFor(() => screen.getByText("mesh-1"));

    mockSend.mockResolvedValueOnce({ virtualNodes: [] });
    await fireEvent.click(screen.getByText("Virtual Nodes"));
    await waitFor(() => screen.getByText("No virtual nodes found"));

    await fireEvent.click(screen.getByText("Create virtual node"));
    await fireEvent.input(within(openDialog()).getByLabelText("Virtual node name"), {
      target: { value: "node-1" },
    });
    await fireEvent.input(within(openDialog()).getByLabelText("Spec (JSON)"), {
      target: { value: "{not json" },
    });

    const callsBefore = mockSend.mock.calls.length;
    await fireEvent.click(within(openDialog()).getByRole("button", { name: "Create" }));

    expect(within(openDialog()).getByText("Spec must be valid JSON.")).toBeInTheDocument();
    expect(mockSend).toHaveBeenCalledTimes(callsBefore);
  });

  it("lists routes nested under a virtual router and creates one", async () => {
    mockSend.mockResolvedValueOnce({ meshes: [exampleMesh] });
    render(AppMeshPage);
    await waitFor(() => screen.getByText("mesh-1"));

    mockSend.mockResolvedValueOnce({
      virtualRouters: [{ meshName: "mesh-1", virtualRouterName: "router-1" }],
    });
    await fireEvent.click(screen.getByText("Virtual Routers"));
    await waitFor(() => screen.getByText("router-1"));

    mockSend.mockResolvedValueOnce({
      virtualRouter: {
        meshName: "mesh-1",
        virtualRouterName: "router-1",
        spec: {},
        status: { status: "ACTIVE" },
      },
    });
    mockSend.mockResolvedValueOnce({ routes: [] });

    await fireEvent.click(screen.getByTitle("View"));
    await waitFor(() => within(openDialog()).getByText("No routes"));

    await fireEvent.click(within(openDialog()).getByText("Add route"));
    await fireEvent.input(within(openDialog()).getByLabelText("Route name"), {
      target: { value: "route-1" },
    });
    await fireEvent.input(within(openDialog()).getByLabelText("Spec (JSON)"), {
      target: { value: '{"httpRoute": {}}' },
    });

    mockSend.mockResolvedValueOnce({
      route: { meshName: "mesh-1", virtualRouterName: "router-1", routeName: "route-1" },
    });
    mockSend.mockResolvedValueOnce({
      routes: [{ meshName: "mesh-1", virtualRouterName: "router-1", routeName: "route-1" }],
    });

    await fireEvent.click(within(openDialog()).getByRole("button", { name: "Create route" }));
    await waitFor(() => within(openDialog()).getByText("route-1"));

    const createRouteCall = mockSend.mock.calls[4][0];
    expect(createRouteCall.input).toEqual({
      meshName: "mesh-1",
      virtualRouterName: "router-1",
      routeName: "route-1",
      spec: { httpRoute: {} },
    });
  });

  it("deletes a nested route after confirming", async () => {
    mockSend.mockResolvedValueOnce({ meshes: [exampleMesh] });
    render(AppMeshPage);
    await waitFor(() => screen.getByText("mesh-1"));

    mockSend.mockResolvedValueOnce({
      virtualRouters: [{ meshName: "mesh-1", virtualRouterName: "router-1" }],
    });
    await fireEvent.click(screen.getByText("Virtual Routers"));
    await waitFor(() => screen.getByText("router-1"));

    mockSend.mockResolvedValueOnce({
      virtualRouter: {
        meshName: "mesh-1",
        virtualRouterName: "router-1",
        spec: {},
        status: { status: "ACTIVE" },
      },
    });
    mockSend.mockResolvedValueOnce({
      routes: [{ meshName: "mesh-1", virtualRouterName: "router-1", routeName: "route-1" }],
    });

    await fireEvent.click(screen.getByTitle("View"));
    await waitFor(() => within(openDialog()).getByText("route-1"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ routes: [] });

    await fireEvent.click(within(openDialog()).getByTitle("Delete route"));
    expect(confirmDestructive).toHaveBeenCalled();

    await waitFor(() => within(openDialog()).getByText("No routes"));

    const deleteCall = mockSend.mock.calls[4][0];
    expect(deleteCall.input).toEqual({
      meshName: "mesh-1",
      virtualRouterName: "router-1",
      routeName: "route-1",
    });
  });
});
