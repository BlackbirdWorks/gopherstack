import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/svelte';
import SecurityHubPage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getSecurityHubClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: { success: vi.fn(), error: vi.fn() }
}));

describe('Security Hub Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockRejectedValueOnce(new Error('not enabled')).mockResolvedValueOnce({ Findings: [] });
		render(SecurityHubPage);
		expect(screen.getByText('Security Hub')).toBeInTheDocument();
	});

	it('shows tabs', () => {
		mockSend.mockRejectedValueOnce(new Error('not enabled'));
		render(SecurityHubPage);
		expect(screen.getByText('Findings')).toBeInTheDocument();
		expect(screen.getByText('Insights')).toBeInTheDocument();
	});

	it('shows not enabled state', async () => {
		mockSend.mockRejectedValueOnce(new Error('Hub not enabled'));
		render(SecurityHubPage);
		await waitFor(() => {
			expect(screen.getByText('Security Hub is not enabled')).toBeInTheDocument();
		});
	});

	it('displays findings when enabled', async () => {
		mockSend
			.mockResolvedValueOnce({}) // DescribeHub
			.mockResolvedValueOnce({
				Findings: [
					{
						Id: 'finding-1',
						Title: 'Security group allows unrestricted access',
						Severity: { Label: 'HIGH' },
						ProductName: 'Security Hub',
						Workflow: { Status: 'NEW' },
						Types: ['Software and Configuration Checks']
					}
				]
			});

		render(SecurityHubPage);

		await waitFor(() => {
			expect(screen.getByText('Security group allows unrestricted access')).toBeInTheDocument();
		});
	});

	it('shows empty findings state', async () => {
		mockSend.mockResolvedValueOnce({}).mockResolvedValueOnce({ Findings: [] });
		render(SecurityHubPage);
		await waitFor(() => {
			expect(screen.getByText('No findings found')).toBeInTheDocument();
		});
	});
});
