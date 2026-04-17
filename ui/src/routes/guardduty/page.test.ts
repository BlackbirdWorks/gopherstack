import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/svelte';
import GuardDutyPage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getGuardDutyClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: { success: vi.fn(), error: vi.fn() }
}));

describe('GuardDuty Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValueOnce({ DetectorIds: [] });
		render(GuardDutyPage);
		expect(screen.getByText('GuardDuty')).toBeInTheDocument();
	});

	it('shows tabs', () => {
		mockSend.mockResolvedValueOnce({ DetectorIds: [] });
		render(GuardDutyPage);
		expect(screen.getByText('Detectors')).toBeInTheDocument();
		expect(screen.getByText('Findings')).toBeInTheDocument();
	});

	it('shows empty state when no detectors', async () => {
		mockSend.mockResolvedValueOnce({ DetectorIds: [] });
		render(GuardDutyPage);
		await waitFor(() => {
			expect(screen.getByText('GuardDuty is not enabled')).toBeInTheDocument();
		});
	});

	it('displays detectors', async () => {
		mockSend
			.mockResolvedValueOnce({ DetectorIds: ['det-abc123'] })
			.mockResolvedValueOnce({
				DetectorId: 'det-abc123',
				Status: 'ENABLED',
				CreatedAt: '2024-01-01',
				UpdatedAt: '2024-01-15'
			});

		render(GuardDutyPage);

		await waitFor(() => {
			expect(screen.getByText('det-abc123')).toBeInTheDocument();
		});
	});

	it('shows enable button', () => {
		mockSend.mockResolvedValueOnce({ DetectorIds: [] });
		render(GuardDutyPage);
		expect(screen.getByText('Enable GuardDuty')).toBeInTheDocument();
	});
});
