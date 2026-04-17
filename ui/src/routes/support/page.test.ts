import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/svelte';
import SupportPage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getSupportClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() }
}));

describe('Support Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValue({ cases: [], severityLevels: [], services: [] });
		render(SupportPage);
		expect(screen.getByText('AWS Support')).toBeInTheDocument();
	});

	it('shows stat cards', () => {
		mockSend.mockResolvedValue({ cases: [] });
		render(SupportPage);
		expect(screen.getByText('Total Cases')).toBeInTheDocument();
	});

	it('shows search input', () => {
		mockSend.mockResolvedValue({ cases: [] });
		render(SupportPage);
		expect(screen.getByPlaceholderText(/search/i)).toBeInTheDocument();
	});

	it('shows empty state', async () => {
		mockSend.mockResolvedValue({ cases: [], severityLevels: [], services: [] });
		render(SupportPage);
		await waitFor(() => {
			expect(screen.getByText(/no support cases/i)).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('displays loaded cases', async () => {
		mockSend.mockResolvedValue({
			cases: [{ caseId: 'case-123', subject: 'My Support Issue', status: 'pending-customer-action', serviceCode: 'amazon-ec2' }],
			severityLevels: [], services: []
		});
		render(SupportPage);
		await waitFor(() => {
			expect(screen.getByText('My Support Issue')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('shows refresh button', () => {
		mockSend.mockResolvedValue({ cases: [] });
		render(SupportPage);
		expect(screen.getByTitle('Refresh')).toBeInTheDocument();
	});

	it('shows AWS Services tab', () => {
		mockSend.mockResolvedValue({ cases: [] });
		render(SupportPage);
		expect(screen.getByText('AWS Services')).toBeInTheDocument();
	});

	it('shows Open and Resolved stats', () => {
		mockSend.mockResolvedValue({ cases: [] });
		render(SupportPage);
		expect(screen.getByText('Open')).toBeInTheDocument();
		expect(screen.getByText('Resolved')).toBeInTheDocument();
	});
});
