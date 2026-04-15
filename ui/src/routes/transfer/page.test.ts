import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/svelte';
import TransferPage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getTransferClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: { success: vi.fn(), error: vi.fn() }
}));

describe('Transfer Family Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValueOnce({ Servers: [] });
		render(TransferPage);
		expect(screen.getByText('Transfer Family')).toBeInTheDocument();
	});

	it('shows create server button', () => {
		mockSend.mockResolvedValueOnce({ Servers: [] });
		render(TransferPage);
		expect(screen.getByText('Create Server')).toBeInTheDocument();
	});

	it('displays servers', async () => {
		mockSend.mockResolvedValueOnce({
			Servers: [
				{
					ServerId: 's-abc123456',
					Domain: 'S3',
					EndpointType: 'PUBLIC',
					State: 'ONLINE',
					UserCount: 3
				}
			]
		});

		render(TransferPage);

		await waitFor(() => {
			expect(screen.getByText('s-abc123456')).toBeInTheDocument();
		});
	});

	it('shows create server modal', async () => {
		mockSend.mockResolvedValueOnce({ Servers: [] });
		render(TransferPage);
		await fireEvent.click(screen.getByText('Create Server'));
		expect(screen.getByText('Create Transfer Server')).toBeInTheDocument();
	});

	it('displays empty state', async () => {
		mockSend.mockResolvedValueOnce({ Servers: [] });
		render(TransferPage);
		await waitFor(() => {
			expect(screen.getByText('No Transfer Family servers found')).toBeInTheDocument();
		});
	});
});
