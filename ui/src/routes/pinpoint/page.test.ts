import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/svelte';
import PinpointPage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getPinpointClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() }
}));

describe('Pinpoint Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValue({ ApplicationsResponse: { Item: [] } });
		render(PinpointPage);
		expect(screen.getByText('Amazon Pinpoint')).toBeInTheDocument();
	});

	it('shows stat cards', () => {
		mockSend.mockResolvedValue({ ApplicationsResponse: { Item: [] } });
		render(PinpointPage);
		expect(screen.getByText('Applications')).toBeInTheDocument();
	});

	it('shows search input', () => {
		mockSend.mockResolvedValue({ ApplicationsResponse: { Item: [] } });
		render(PinpointPage);
		expect(screen.getByPlaceholderText(/search/i)).toBeInTheDocument();
	});

	it('shows empty state when no applications', async () => {
		mockSend.mockResolvedValue({ ApplicationsResponse: { Item: [] } });
		render(PinpointPage);
		await waitFor(() => {
			expect(screen.getByText(/no applications/i)).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('displays loaded applications', async () => {
		mockSend.mockResolvedValue({
			ApplicationsResponse: { Item: [{ Id: 'app-abc', Name: 'My Marketing App' }] }
		});
		render(PinpointPage);
		await waitFor(() => {
			expect(screen.getByText('My Marketing App')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('shows refresh button', () => {
		mockSend.mockResolvedValue({ ApplicationsResponse: { Item: [] } });
		render(PinpointPage);
		expect(screen.getByTitle('Refresh')).toBeInTheDocument();
	});

	it('shows Campaigns tab', () => {
		mockSend.mockResolvedValue({ ApplicationsResponse: { Item: [] } });
		render(PinpointPage);
		expect(screen.getByText('Campaigns')).toBeInTheDocument();
	});

	it('shows Segments tab', () => {
		mockSend.mockResolvedValue({ ApplicationsResponse: { Item: [] } });
		render(PinpointPage);
		expect(screen.getByText('Segments')).toBeInTheDocument();
	});
});
