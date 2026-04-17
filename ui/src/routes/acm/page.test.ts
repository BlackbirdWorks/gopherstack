import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/svelte';
import ACMPage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getACMClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: { success: vi.fn(), error: vi.fn() }
}));

describe('ACM Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValueOnce({ CertificateSummaryList: [] });
		render(ACMPage);
		expect(screen.getByText('Certificate Manager')).toBeInTheDocument();
	});

	it('shows request certificate button', () => {
		mockSend.mockResolvedValueOnce({ CertificateSummaryList: [] });
		render(ACMPage);
		expect(screen.getByText('Request Certificate')).toBeInTheDocument();
	});

	it('displays certificates', async () => {
		mockSend.mockResolvedValueOnce({
			CertificateSummaryList: [
				{
					CertificateArn: 'arn:acm:cert/1',
					DomainName: 'example.com',
					Status: 'ISSUED',
					Type: 'AMAZON_ISSUED'
				}
			]
		});

		render(ACMPage);

		await waitFor(() => {
			expect(screen.getByText('example.com')).toBeInTheDocument();
		});
	});

	it('shows request modal', async () => {
		mockSend.mockResolvedValueOnce({ CertificateSummaryList: [] });
		render(ACMPage);
		await fireEvent.click(screen.getByText('Request Certificate'));
		expect(screen.getByText('Request Certificate', { selector: 'h2' })).toBeInTheDocument();
	});

	it('shows empty state', async () => {
		mockSend.mockResolvedValueOnce({ CertificateSummaryList: [] });
		render(ACMPage);
		await waitFor(() => {
			expect(screen.getByText('No certificates found')).toBeInTheDocument();
		});
	});
});
