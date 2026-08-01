import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import LakeFormationPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getLakeFormationClient: () => ({ send: mockSend }),
}));

const toastError = vi.fn();
const toastSuccess = vi.fn();
vi.mock("svelte-sonner", () => ({
  toast: {
    success: (...a: unknown[]) => toastSuccess(...a),
    error: (...a: unknown[]) => toastError(...a),
  },
}));

const exampleResource = {
  ResourceArn: "arn:aws:s3:::my-bucket",
  RoleArn: "arn:aws:iam::123456789012:role/lf-role",
};

const exampleTag = {
  TagKey: "environment",
  TagValues: ["dev", "prod"],
};

const examplePermission = {
  Principal: { DataLakePrincipalIdentifier: "arn:aws:iam::123456789012:user/alice" },
  Resource: { Database: { Name: "my_database" } },
  Permissions: ["SELECT"],
};

const exampleTransaction = {
  TransactionId: "txn-1",
  TransactionStatus: "ACTIVE",
};

const exampleFilter = {
  Name: "row-filter-1",
  DatabaseName: "my_database",
  TableName: "my_table",
  TableCatalogId: "123456789012",
};

const exampleExpression = {
  Name: "prod-data-access",
  Description: "Access to prod",
  Expression: [{ TagKey: "environment", TagValues: ["prod"] }],
};

describe("Lake Formation Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title", () => {
    mockSend.mockResolvedValueOnce({ ResourceInfoList: [] });
    render(LakeFormationPage);
    expect(screen.getByText("Lake Formation")).toBeInTheDocument();
  });

  it("shows all tabs", () => {
    mockSend.mockResolvedValueOnce({ ResourceInfoList: [] });
    render(LakeFormationPage);
    expect(screen.getByText("Resources")).toBeInTheDocument();
    expect(screen.getByText("LF Tags")).toBeInTheDocument();
    expect(screen.getByText("Permissions")).toBeInTheDocument();
    expect(screen.getByText("Transactions")).toBeInTheDocument();
    expect(screen.getByText("Data Filters")).toBeInTheDocument();
    expect(screen.getByText("Expressions")).toBeInTheDocument();
  });

  it("loads resources on mount with an empty ListResourcesCommand input", async () => {
    mockSend.mockResolvedValueOnce({ ResourceInfoList: [exampleResource] });
    render(LakeFormationPage);
    await waitFor(() => {
      expect(screen.getByRole("cell", { name: exampleResource.ResourceArn })).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenCalledWith(expect.objectContaining({ input: {} }));
    expect(screen.getByRole("cell", { name: exampleResource.RoleArn })).toBeInTheDocument();
  });

  it("registers a resource via the modal", async () => {
    mockSend.mockResolvedValueOnce({ ResourceInfoList: [] });
    render(LakeFormationPage);
    await waitFor(() => screen.getByText("No resources registered"));

    // Both the header action and the empty-state CTA are labeled "Register
    // Resource" and open the same modal -- either is fine to click.
    await fireEvent.click(screen.getAllByText("Register Resource")[0]);
    await fireEvent.input(screen.getByLabelText("Resource ARN"), {
      target: { value: "arn:aws:s3:::my-bucket" },
    });
    await fireEvent.input(screen.getByLabelText("Role ARN"), {
      target: { value: "arn:aws:iam::123456789012:role/lf-role" },
    });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ ResourceInfoList: [exampleResource] });

    await fireEvent.click(screen.getByRole("button", { name: "Register" }));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: exampleResource.ResourceArn })).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({
        input: {
          ResourceArn: "arn:aws:s3:::my-bucket",
          RoleArn: "arn:aws:iam::123456789012:role/lf-role",
        },
      }),
    );
  });

  it("deregisters a resource via the page's own confirm dialog when confirmed", async () => {
    mockSend.mockResolvedValueOnce({ ResourceInfoList: [exampleResource] });
    render(LakeFormationPage);
    await waitFor(() => screen.getByRole("cell", { name: exampleResource.ResourceArn }));

    await fireEvent.click(screen.getByTitle("Deregister"));
    // This page implements its own inline confirm modal rather than the
    // shared confirm-dialog module -- assert the dialog actually appeared
    // with the resource-specific message before confirming.
    expect(
      screen.getByText(
        `Deregister resource "${exampleResource.ResourceArn}"? This cannot be undone.`,
      ),
    ).toBeInTheDocument();

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ ResourceInfoList: [] });

    await fireEvent.click(screen.getByRole("button", { name: "Confirm" }));

    await waitFor(() => screen.getByText("No resources registered"));
    expect(mockSend).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({ input: { ResourceArn: exampleResource.ResourceArn } }),
    );
  });

  it("does not deregister when the confirm dialog is cancelled", async () => {
    mockSend.mockResolvedValueOnce({ ResourceInfoList: [exampleResource] });
    render(LakeFormationPage);
    await waitFor(() => screen.getByRole("cell", { name: exampleResource.ResourceArn }));

    await fireEvent.click(screen.getByTitle("Deregister"));
    await fireEvent.click(screen.getByRole("button", { name: "Cancel" }));

    // Only the initial ListResources call -- no Deregister, no reload, and
    // the dialog itself is gone.
    expect(mockSend).toHaveBeenCalledTimes(1);
    expect(
      screen.queryByText(
        `Deregister resource "${exampleResource.ResourceArn}"? This cannot be undone.`,
      ),
    ).not.toBeInTheDocument();
    expect(screen.getByRole("cell", { name: exampleResource.ResourceArn })).toBeInTheDocument();
  });

  it("shows a toast with the AWS error message when a load fails", async () => {
    const error = Object.assign(new Error("Access denied."), {
      name: "AccessDeniedException",
      $metadata: { httpStatusCode: 403 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(LakeFormationPage);

    await waitFor(() => {
      expect(toastError).toHaveBeenCalledWith("Failed to load resources: Access denied.");
    });
  });

  it("switches to the LF Tags tab, lists tags, and creates one from comma-separated values", async () => {
    mockSend.mockResolvedValueOnce({ ResourceInfoList: [] });
    render(LakeFormationPage);
    await waitFor(() => screen.getByText("No resources registered"));

    mockSend.mockResolvedValueOnce({ LFTags: [exampleTag] });
    await fireEvent.click(screen.getByText("LF Tags"));
    await waitFor(() => screen.getByRole("cell", { name: "environment" }));
    expect(screen.getByText("dev")).toBeInTheDocument();
    expect(screen.getByText("prod")).toBeInTheDocument();

    await fireEvent.click(screen.getByText("Create Tag"));
    await fireEvent.input(screen.getByLabelText("Tag Key"), { target: { value: "team" } });
    await fireEvent.input(screen.getByLabelText("Allowed Values (comma-separated)"), {
      target: { value: "alpha, beta" },
    });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      LFTags: [exampleTag, { TagKey: "team", TagValues: ["alpha", "beta"] }],
    });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "team" })).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({ input: { TagKey: "team", TagValues: ["alpha", "beta"] } }),
    );
  });

  it("edits an LF tag's values via UpdateLFTagCommand", async () => {
    mockSend.mockResolvedValueOnce({ ResourceInfoList: [] });
    render(LakeFormationPage);
    await waitFor(() => screen.getByText("No resources registered"));

    mockSend.mockResolvedValueOnce({ LFTags: [exampleTag] });
    await fireEvent.click(screen.getByText("LF Tags"));
    await waitFor(() => screen.getByRole("cell", { name: "environment" }));

    await fireEvent.click(screen.getByText("Edit"));
    await fireEvent.input(screen.getByLabelText("Values to Add (comma-separated)"), {
      target: { value: "staging" },
    });
    await fireEvent.input(screen.getByLabelText("Values to Remove (comma-separated)"), {
      target: { value: "dev" },
    });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      LFTags: [{ TagKey: "environment", TagValues: ["prod", "staging"] }],
    });

    await fireEvent.click(screen.getByRole("button", { name: "Save" }));

    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        3,
        expect.objectContaining({
          input: { TagKey: "environment", TagValuesToAdd: ["staging"], TagValuesToDelete: ["dev"] },
        }),
      );
    });
  });

  it("deletes an LF tag after confirming", async () => {
    mockSend.mockResolvedValueOnce({ ResourceInfoList: [] });
    render(LakeFormationPage);
    await waitFor(() => screen.getByText("No resources registered"));

    mockSend.mockResolvedValueOnce({ LFTags: [exampleTag] });
    await fireEvent.click(screen.getByText("LF Tags"));
    await waitFor(() => screen.getByRole("cell", { name: "environment" }));

    await fireEvent.click(screen.getByTitle("Delete tag"));
    expect(
      screen.getByText(
        'Delete LF tag "environment"? All resources tagged with this key will lose the tag.',
      ),
    ).toBeInTheDocument();

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ LFTags: [] });

    await fireEvent.click(screen.getByRole("button", { name: "Confirm" }));

    await waitFor(() => screen.getByText("No LF tags defined"));
    expect(mockSend).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({ input: { TagKey: "environment" } }),
    );
  });

  it("filters LF tags by tag key without an extra API call", async () => {
    mockSend.mockResolvedValueOnce({ ResourceInfoList: [] });
    render(LakeFormationPage);
    await waitFor(() => screen.getByText("No resources registered"));

    mockSend.mockResolvedValueOnce({
      LFTags: [exampleTag, { TagKey: "team", TagValues: ["alpha"] }],
    });
    await fireEvent.click(screen.getByText("LF Tags"));
    await waitFor(() => screen.getByRole("cell", { name: "environment" }));

    const callsBeforeFilter = mockSend.mock.calls.length;
    await fireEvent.input(screen.getByPlaceholderText("Filter by tag key…"), {
      target: { value: "team" },
    });

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "team" })).toBeInTheDocument();
      expect(screen.queryByRole("cell", { name: "environment" })).not.toBeInTheDocument();
    });
    expect(mockSend.mock.calls.length).toBe(callsBeforeFilter);
  });

  it("switches to Permissions, lists a grant, and revokes it after confirming", async () => {
    mockSend.mockResolvedValueOnce({ ResourceInfoList: [] });
    render(LakeFormationPage);
    await waitFor(() => screen.getByText("No resources registered"));

    mockSend.mockResolvedValueOnce({ PrincipalResourcePermissions: [examplePermission] });
    await fireEvent.click(screen.getByText("Permissions"));
    await waitFor(() => screen.getByText("SELECT"));
    // Resource description is rendered by describeResource(), a plain
    // function (this page has no snippet-column architecture) -- confirm
    // it actually produced text instead of leaving the cell blank.
    expect(screen.getByText("DB: my_database")).toBeInTheDocument();

    await fireEvent.click(screen.getByTitle("Revoke"));
    expect(
      screen.getByText('Revoke permissions for "arn:aws:iam::123456789012:user/alice"?'),
    ).toBeInTheDocument();

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ PrincipalResourcePermissions: [] });

    await fireEvent.click(screen.getByRole("button", { name: "Confirm" }));

    await waitFor(() => screen.getByText("No permissions granted"));
    expect(mockSend).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({
        input: {
          Principal: examplePermission.Principal,
          Resource: examplePermission.Resource,
          Permissions: examplePermission.Permissions,
        },
      }),
    );
  });

  it("grants a database permission with comma-separated permission lists", async () => {
    mockSend.mockResolvedValueOnce({ ResourceInfoList: [] });
    render(LakeFormationPage);
    await waitFor(() => screen.getByText("No resources registered"));

    mockSend.mockResolvedValueOnce({ PrincipalResourcePermissions: [] });
    await fireEvent.click(screen.getByText("Permissions"));
    await waitFor(() => screen.getByText("No permissions granted"));

    // Both the header action and the empty-state CTA are labeled "Grant
    // Permission" and open the same modal -- either is fine to click.
    await fireEvent.click(screen.getAllByText("Grant Permission")[0]);
    await fireEvent.input(screen.getByLabelText("Principal ARN (IAM user/role/group)"), {
      target: { value: "arn:aws:iam::123456789012:user/bob" },
    });
    // Database is the default resource type -- fill in its field.
    await fireEvent.input(screen.getByLabelText("Database Name"), { target: { value: "sales" } });
    await fireEvent.input(
      screen.getByLabelText("Permissions (comma-separated, e.g. SELECT,DESCRIBE)"),
      {
        target: { value: "SELECT, DESCRIBE" },
      },
    );

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      PrincipalResourcePermissions: [
        {
          Principal: { DataLakePrincipalIdentifier: "arn:aws:iam::123456789012:user/bob" },
          Resource: { Database: { Name: "sales" } },
          Permissions: ["SELECT", "DESCRIBE"],
        },
      ],
    });

    await fireEvent.click(screen.getByRole("button", { name: "Grant" }));

    await waitFor(() => {
      expect(screen.getByText("arn:aws:iam::123456789012:user/bob")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({
        input: {
          Principal: { DataLakePrincipalIdentifier: "arn:aws:iam::123456789012:user/bob" },
          Resource: { Database: { Name: "sales" } },
          Permissions: ["SELECT", "DESCRIBE"],
          PermissionsWithGrantOption: undefined,
        },
      }),
    );
  });

  it("switches to Transactions, lists an active transaction, and commits it", async () => {
    mockSend.mockResolvedValueOnce({ ResourceInfoList: [] });
    render(LakeFormationPage);
    await waitFor(() => screen.getByText("No resources registered"));

    mockSend.mockResolvedValueOnce({ Transactions: [exampleTransaction] });
    await fireEvent.click(screen.getByText("Transactions"));
    await waitFor(() => screen.getByText("txn-1"));
    expect(mockSend).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({ input: { StatusFilter: undefined } }),
    );

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      Transactions: [{ ...exampleTransaction, TransactionStatus: "COMMITTED" }],
    });

    await fireEvent.click(screen.getByRole("button", { name: "Commit" }));

    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        3,
        expect.objectContaining({ input: { TransactionId: "txn-1" } }),
      );
    });
  });

  it("cancels an active transaction after confirming", async () => {
    mockSend.mockResolvedValueOnce({ ResourceInfoList: [] });
    render(LakeFormationPage);
    await waitFor(() => screen.getByText("No resources registered"));

    mockSend.mockResolvedValueOnce({ Transactions: [exampleTransaction] });
    await fireEvent.click(screen.getByText("Transactions"));
    await waitFor(() => screen.getByText("txn-1"));

    await fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
    expect(
      screen.getByText('Cancel transaction "txn-1"? Active writes will be rolled back.'),
    ).toBeInTheDocument();

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ Transactions: [] });

    await fireEvent.click(screen.getByRole("button", { name: "Confirm" }));

    await waitFor(() => screen.getByText("No transactions"));
    expect(mockSend).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({ input: { TransactionId: "txn-1" } }),
    );
  });

  it("starts a new transaction from the header button", async () => {
    mockSend.mockResolvedValueOnce({ ResourceInfoList: [] });
    render(LakeFormationPage);
    await waitFor(() => screen.getByText("No resources registered"));

    mockSend.mockResolvedValueOnce({ Transactions: [] });
    await fireEvent.click(screen.getByText("Transactions"));
    await waitFor(() => screen.getByText("No transactions"));

    mockSend.mockResolvedValueOnce({ TransactionId: "txn-new" });
    mockSend.mockResolvedValueOnce({
      Transactions: [{ TransactionId: "txn-new", TransactionStatus: "ACTIVE" }],
    });

    // Both the header action and the empty-state CTA are labeled "Start
    // Transaction" and call the same handler directly (no modal) -- either
    // is fine to click.
    await fireEvent.click(screen.getAllByRole("button", { name: "Start Transaction" })[0]);

    await waitFor(() => screen.getByText("txn-new"));
    expect(mockSend).toHaveBeenNthCalledWith(3, expect.objectContaining({ input: {} }));
  });

  it("switches to Data Filters, lists a filter, and creates one", async () => {
    mockSend.mockResolvedValueOnce({ ResourceInfoList: [] });
    render(LakeFormationPage);
    await waitFor(() => screen.getByText("No resources registered"));

    mockSend.mockResolvedValueOnce({ DataCellsFilters: [exampleFilter] });
    await fireEvent.click(screen.getByText("Data Filters"));
    await waitFor(() => screen.getByRole("cell", { name: "row-filter-1" }));

    await fireEvent.click(screen.getByText("Create Filter"));
    await fireEvent.input(screen.getByLabelText("Catalog ID"), {
      target: { value: "999999999999" },
    });
    await fireEvent.input(screen.getByLabelText("Database Name"), { target: { value: "db2" } });
    await fireEvent.input(screen.getByLabelText("Table Name"), { target: { value: "tbl2" } });
    await fireEvent.input(screen.getByLabelText("Filter Name"), { target: { value: "filter-2" } });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      DataCellsFilters: [
        exampleFilter,
        {
          Name: "filter-2",
          DatabaseName: "db2",
          TableName: "tbl2",
          TableCatalogId: "999999999999",
        },
      ],
    });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "filter-2" })).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({
        input: {
          TableData: {
            TableCatalogId: "999999999999",
            DatabaseName: "db2",
            TableName: "tbl2",
            Name: "filter-2",
          },
        },
      }),
    );
  });

  it("deletes a data filter after confirming", async () => {
    mockSend.mockResolvedValueOnce({ ResourceInfoList: [] });
    render(LakeFormationPage);
    await waitFor(() => screen.getByText("No resources registered"));

    mockSend.mockResolvedValueOnce({ DataCellsFilters: [exampleFilter] });
    await fireEvent.click(screen.getByText("Data Filters"));
    await waitFor(() => screen.getByRole("cell", { name: "row-filter-1" }));

    await fireEvent.click(screen.getByTitle("Delete"));
    expect(screen.getByText('Delete data cells filter "row-filter-1"?')).toBeInTheDocument();

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ DataCellsFilters: [] });

    await fireEvent.click(screen.getByRole("button", { name: "Confirm" }));

    await waitFor(() => screen.getByText("No data cells filters"));
    expect(mockSend).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({
        input: {
          TableCatalogId: exampleFilter.TableCatalogId,
          DatabaseName: exampleFilter.DatabaseName,
          TableName: exampleFilter.TableName,
          Name: exampleFilter.Name,
        },
      }),
    );
  });

  it("switches to Expressions, lists one, and creates a new expression", async () => {
    mockSend.mockResolvedValueOnce({ ResourceInfoList: [] });
    render(LakeFormationPage);
    await waitFor(() => screen.getByText("No resources registered"));

    mockSend.mockResolvedValueOnce({ LFTagExpressions: [exampleExpression] });
    await fireEvent.click(screen.getByText("Expressions"));
    await waitFor(() => screen.getByRole("cell", { name: "prod-data-access" }));

    await fireEvent.click(screen.getByText("Create Expression"));
    await fireEvent.input(screen.getByLabelText("Expression Name"), {
      target: { value: "dev-access" },
    });
    await fireEvent.input(screen.getByLabelText("Tag Key"), { target: { value: "environment" } });
    await fireEvent.input(screen.getByLabelText("Tag Values (comma-separated)"), {
      target: { value: "dev" },
    });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      LFTagExpressions: [
        exampleExpression,
        { Name: "dev-access", Expression: [{ TagKey: "environment", TagValues: ["dev"] }] },
      ],
    });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "dev-access" })).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({
        input: {
          Name: "dev-access",
          Description: undefined,
          Expression: [{ TagKey: "environment", TagValues: ["dev"] }],
        },
      }),
    );
  });

  it("deletes an LF tag expression after confirming", async () => {
    mockSend.mockResolvedValueOnce({ ResourceInfoList: [] });
    render(LakeFormationPage);
    await waitFor(() => screen.getByText("No resources registered"));

    mockSend.mockResolvedValueOnce({ LFTagExpressions: [exampleExpression] });
    await fireEvent.click(screen.getByText("Expressions"));
    await waitFor(() => screen.getByRole("cell", { name: "prod-data-access" }));

    await fireEvent.click(screen.getByTitle("Delete"));
    expect(screen.getByText('Delete LF tag expression "prod-data-access"?')).toBeInTheDocument();

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ LFTagExpressions: [] });

    await fireEvent.click(screen.getByRole("button", { name: "Confirm" }));

    await waitFor(() => screen.getByText("No LF tag expressions"));
    expect(mockSend).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({ input: { Name: "prod-data-access" } }),
    );
  });
});
