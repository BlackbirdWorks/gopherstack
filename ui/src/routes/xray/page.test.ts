import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/svelte';
import XRayPage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getXRayClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: { success: vi.fn(), error: vi.fn() }
}));

describe('X-Ray Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValueOnce({ TraceSummaries: [] });
		render(XRayPage);
		expect(screen.getByText('AWS X-Ray')).toBeInTheDocument();
	});

	it('shows subtitle', () => {
		mockSend.mockResolvedValueOnce({ TraceSummaries: [] });
		render(XRayPage);
		expect(screen.getByText('Distributed tracing for application analysis')).toBeInTheDocument();
	});

	it('shows stat cards always', () => {
		mockSend.mockResolvedValueOnce({ TraceSummaries: [] });
		render(XRayPage);
		expect(screen.getByText('Total Traces')).toBeInTheDocument();
		expect(screen.getByText('Faults')).toBeInTheDocument();
		expect(screen.getByText('Errors')).toBeInTheDocument();
		expect(screen.getByText('Avg Duration')).toBeInTheDocument();
	});

	it('shows tabs', () => {
		mockSend.mockResolvedValueOnce({ TraceSummaries: [] });
		render(XRayPage);
		expect(screen.getByText('Traces')).toBeInTheDocument();
		expect(screen.getByText('Groups')).toBeInTheDocument();
	});

	it('displays traces', async () => {
		mockSend.mockResolvedValueOnce({
			TraceSummaries: [
				{
					Id: '1-5f0b2ea2-abc123',
					Duration: 0.342,
					HasFault: false,
					HasError: false,
					HasThrottle: false,
					Http: { HttpStatus: 200, HttpURL: '/api/users' }
				}
			]
		});

		render(XRayPage);

		await waitFor(() => {
			expect(screen.getByText('1-5f0b2ea2-abc123')).toBeInTheDocument();
		});
	});

	it('shows empty state', async () => {
		mockSend.mockResolvedValueOnce({ TraceSummaries: [] });
		render(XRayPage);
		await waitFor(() => {
			expect(screen.getByText('No traces found')).toBeInTheDocument();
		});
	});

	it('switches to groups tab', async () => {
		mockSend
			.mockResolvedValueOnce({ TraceSummaries: [] })
			.mockResolvedValueOnce({ Groups: [] });
		render(XRayPage);
		await fireEvent.click(screen.getByText('Groups'));
		await waitFor(() => {
			expect(screen.getByText('No groups found')).toBeInTheDocument();
		});
	});
});

