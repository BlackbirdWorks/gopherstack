import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/svelte';
import CognitoPage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
getCognitoIDPClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() }
}));

const mockPool = {
Id: 'us-east-1_abc123',
Name: 'MyUserPool',
Status: 'Enabled',
CreationDate: new Date('2024-01-01'),
LastModifiedDate: new Date('2024-06-01')
};

describe('Cognito Page', () => {
beforeEach(() => {
vi.clearAllMocks();
mockSend.mockReset();
});

it('renders page title', () => {
mockSend.mockResolvedValue({ UserPools: [] });
render(CognitoPage);
expect(screen.getByText('Cognito User Pools')).toBeInTheDocument();
});

it('shows Create Pool button', () => {
mockSend.mockResolvedValue({ UserPools: [] });
render(CognitoPage);
expect(screen.getByText('Create Pool')).toBeInTheDocument();
});

it('shows empty state', async () => {
mockSend.mockResolvedValue({ UserPools: [] });
render(CognitoPage);
await waitFor(() => {
expect(screen.getByText('No Cognito user pools found')).toBeInTheDocument();
}, { timeout: 3000 });
});

it('displays loaded user pools', async () => {
mockSend.mockResolvedValueOnce({ UserPools: [mockPool] });
render(CognitoPage);
await waitFor(() => {
expect(screen.getByText('MyUserPool')).toBeInTheDocument();
}, { timeout: 3000 });
expect(screen.getByText('us-east-1_abc123')).toBeInTheDocument();
});

it('filters user pools via search', async () => {
mockSend.mockResolvedValueOnce({
UserPools: [
mockPool,
{ Id: 'us-east-1_xyz', Name: 'AdminPool', Status: 'Enabled' }
]
});
render(CognitoPage);
await waitFor(() => {
expect(screen.getByText('MyUserPool')).toBeInTheDocument();
}, { timeout: 3000 });
const searchInput = screen.getByPlaceholderText('Search user pools...');
await fireEvent.input(searchInput, { target: { value: 'Admin' } });
await waitFor(() => {
expect(screen.queryByText('MyUserPool')).not.toBeInTheDocument();
});
expect(screen.getByText('AdminPool')).toBeInTheDocument();
});

it('shows error toast on load failure', async () => {
mockSend.mockRejectedValueOnce(new Error('forbidden'));
render(CognitoPage);
const { toast } = await import('svelte-sonner');
await waitFor(() => {
expect(vi.mocked(toast.error)).toHaveBeenCalled();
}, { timeout: 3000 });
});

it('shows User identity and access management subtitle', () => {
mockSend.mockResolvedValue({ UserPools: [] });
render(CognitoPage);
expect(screen.getByText('User identity and access management')).toBeInTheDocument();
});

it('shows pool count summary', async () => {
mockSend.mockResolvedValueOnce({ UserPools: [mockPool] });
render(CognitoPage);
await waitFor(() => {
expect(screen.getByText('MyUserPool')).toBeInTheDocument();
}, { timeout: 3000 });
expect(screen.getByText(/1 pool/i)).toBeInTheDocument();
});
});
