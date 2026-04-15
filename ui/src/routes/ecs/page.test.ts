import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/svelte';
import ECSPage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
getECSClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() }
}));

describe('ECS Page', () => {
beforeEach(() => {
vi.clearAllMocks();
mockSend.mockReset();
});

it('renders page title', () => {
mockSend.mockResolvedValue({ clusterArns: [] });
render(ECSPage);
expect(screen.getByText('ECS Clusters')).toBeInTheDocument();
});

it('shows Create Cluster button', () => {
mockSend.mockResolvedValue({ clusterArns: [] });
render(ECSPage);
expect(screen.getByText('Create Cluster')).toBeInTheDocument();
});

it('shows empty state when no clusters', async () => {
mockSend.mockResolvedValue({ clusterArns: [] });
render(ECSPage);
await waitFor(() => {
expect(screen.getByText('No ECS clusters found')).toBeInTheDocument();
}, { timeout: 3000 });
});

it('displays loaded clusters', async () => {
mockSend.mockResolvedValueOnce({ clusterArns: ['arn:aws:ecs:us-east-1:123:cluster/prod-cluster'] });
mockSend.mockResolvedValueOnce({ serviceArns: [] });
render(ECSPage);
await waitFor(() => {
expect(screen.getByText('prod-cluster')).toBeInTheDocument();
}, { timeout: 3000 });
});

it('loads services when cluster selected', async () => {
mockSend.mockResolvedValueOnce({ clusterArns: ['arn:aws:ecs:us-east-1:123:cluster/prod-cluster'] });
mockSend.mockResolvedValueOnce({
serviceArns: ['arn:aws:ecs:us-east-1:123:service/prod-cluster/api-service']
});
render(ECSPage);
await waitFor(() => {
expect(screen.getByText('api-service')).toBeInTheDocument();
}, { timeout: 3000 });
});

it('shows error toast on load failure', async () => {
mockSend.mockRejectedValueOnce(new Error('network error'));
render(ECSPage);
const { toast } = await import('svelte-sonner');
await waitFor(() => {
expect(vi.mocked(toast.error)).toHaveBeenCalled();
}, { timeout: 3000 });
});

it('shows Elastic Container Service subtitle', () => {
mockSend.mockResolvedValue({ clusterArns: [] });
render(ECSPage);
expect(screen.getByText('Elastic Container Service')).toBeInTheDocument();
});
});
