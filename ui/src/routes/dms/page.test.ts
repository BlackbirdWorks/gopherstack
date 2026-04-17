import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/svelte';
import DMSPage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getDMSClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() }
}));

describe('DMS Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValue({ ReplicationInstances: [], ReplicationTasks: [] });
		render(DMSPage);
		expect(screen.getByText('AWS Database Migration Service')).toBeInTheDocument();
	});

	it('shows stat cards', () => {
		mockSend.mockResolvedValue({ ReplicationInstances: [] });
		render(DMSPage);
		expect(screen.getAllByText('Replication Instances').length).toBeGreaterThanOrEqual(1);
	});

	it('shows search input', () => {
		mockSend.mockResolvedValue({ ReplicationInstances: [] });
		render(DMSPage);
		expect(screen.getByPlaceholderText(/search/i)).toBeInTheDocument();
	});

	it('shows empty state when no instances', async () => {
		mockSend.mockResolvedValue({ ReplicationInstances: [] });
		render(DMSPage);
		await waitFor(() => {
			expect(screen.getByText(/no replication instances/i)).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('displays loaded instances', async () => {
		mockSend.mockResolvedValue({
			ReplicationInstances: [{ ReplicationInstanceIdentifier: 'my-instance', ReplicationInstanceStatus: 'available', ReplicationInstanceClass: 'dms.r5.large' }]
		});
		render(DMSPage);
		await waitFor(() => {
			expect(screen.getByText('my-instance')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('shows refresh button', () => {
		mockSend.mockResolvedValue({ ReplicationInstances: [] });
		render(DMSPage);
		expect(screen.getByTitle('Refresh')).toBeInTheDocument();
	});

	it('shows Replication Instances tab', () => {
		mockSend.mockResolvedValue({ ReplicationInstances: [] });
		render(DMSPage);
		expect(screen.getAllByText('Replication Instances').length).toBeGreaterThanOrEqual(1);
	});

	it('shows Tasks tab', () => {
		mockSend.mockResolvedValue({ ReplicationInstances: [] });
		render(DMSPage);
		expect(screen.getAllByText('Tasks').length).toBeGreaterThanOrEqual(1);
	});
});
