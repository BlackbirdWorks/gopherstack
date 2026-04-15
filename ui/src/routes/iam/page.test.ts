import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/svelte';
import IAMPage from './+page.svelte';

const mockSend = vi.fn();
vi.mock('$lib/aws-client', () => ({ getIAMClient: () => ({ send: mockSend }) }));
vi.mock('svelte-sonner', () => ({ toast: { success: vi.fn(), error: vi.fn(), info: vi.fn() } }));

const mockUser = { UserName: 'alice', UserId: 'AIDA123', Arn: 'arn:aws:iam::123:user/alice', CreateDate: new Date() };
const mockRole = { RoleName: 'AdminRole', RoleId: 'AROA456', Arn: 'arn:aws:iam::123:role/AdminRole' };

describe('IAM Page', () => {
beforeEach(() => { vi.clearAllMocks(); mockSend.mockReset(); });

it('renders page title', () => {
mockSend.mockResolvedValue({ Users: [], Roles: [], Groups: [] });
render(IAMPage);
expect(screen.getByText('IAM')).toBeInTheDocument();
});

it('shows tabs for Users/Roles/Groups', () => {
mockSend.mockResolvedValue({ Users: [], Roles: [], Groups: [] });
render(IAMPage);
expect(screen.getByText('Users')).toBeInTheDocument();
expect(screen.getByText('Roles')).toBeInTheDocument();
expect(screen.getByText('Groups')).toBeInTheDocument();
});

it('shows loaded users', async () => {
mockSend.mockResolvedValueOnce({ Users: [mockUser] });
render(IAMPage);
await waitFor(() => {
expect(screen.getByText('alice')).toBeInTheDocument();
}, { timeout: 3000 });
});

it('shows empty state for users', async () => {
mockSend.mockResolvedValue({ Users: [] });
render(IAMPage);
await waitFor(() => {
expect(screen.getByText('No users found')).toBeInTheDocument();
}, { timeout: 3000 });
});

it('switches to Roles tab and shows roles', async () => {
mockSend.mockResolvedValueOnce({ Users: [] });
mockSend.mockResolvedValueOnce({ Roles: [mockRole] });
render(IAMPage);
await waitFor(() => expect(screen.getByText('No users found')).toBeInTheDocument(), { timeout: 3000 });
await fireEvent.click(screen.getByText('Roles'));
await waitFor(() => {
expect(screen.getByText('AdminRole')).toBeInTheDocument();
}, { timeout: 3000 });
});

it('searches users by name', async () => {
mockSend.mockResolvedValueOnce({ Users: [mockUser, { UserName: 'bob', UserId: 'AIDA999' }] });
render(IAMPage);
await waitFor(() => expect(screen.getByText('alice')).toBeInTheDocument(), { timeout: 3000 });
const searchInput = screen.getByPlaceholderText(/search/i);
await fireEvent.input(searchInput, { target: { value: 'alice' } });
await waitFor(() => expect(screen.queryByText('bob')).not.toBeInTheDocument());
});

it('shows error toast on failure', async () => {
mockSend.mockRejectedValueOnce(new Error('forbidden'));
render(IAMPage);
const { toast } = await import('svelte-sonner');
await waitFor(() => expect(vi.mocked(toast.error)).toHaveBeenCalled(), { timeout: 3000 });
});

it('shows user ARN', async () => {
mockSend.mockResolvedValueOnce({ Users: [mockUser] });
render(IAMPage);
await waitFor(() => expect(screen.getByText('arn:aws:iam::123:user/alice')).toBeInTheDocument(), { timeout: 3000 });
});
});
