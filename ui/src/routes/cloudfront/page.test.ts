import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/svelte';
import CloudFrontPage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getCloudFrontClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() }
}));

describe('CloudFront Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValue({ DistributionList: { Items: [], Quantity: 0 } });
		render(CloudFrontPage);
		expect(screen.getByText('Amazon CloudFront')).toBeInTheDocument();
	});

	it('shows empty state when no distributions', async () => {
		mockSend.mockResolvedValue({ DistributionList: { Items: [], Quantity: 0 } });
		render(CloudFrontPage);
		await waitFor(() => {
			expect(screen.getByText('No distributions found')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('displays loaded distributions', async () => {
		mockSend.mockResolvedValue({
			DistributionList: {
				Items: [
					{ Id: 'E1ABCDEF', DomainName: 'abc.cloudfront.net', Status: 'Deployed', Enabled: true, Comment: 'My CDN', Origins: { Quantity: 1 } },
					{ Id: 'E2GHIJKL', DomainName: 'xyz.cloudfront.net', Status: 'InProgress', Enabled: true, Comment: '', Origins: { Quantity: 2 } }
				],
				Quantity: 2
			}
		});
		render(CloudFrontPage);
		await waitFor(() => {
			expect(screen.getByText('E1ABCDEF')).toBeInTheDocument();
			expect(screen.getByText('E2GHIJKL')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('shows search input', () => {
		mockSend.mockResolvedValue({ DistributionList: { Items: [], Quantity: 0 } });
		render(CloudFrontPage);
		expect(screen.getByPlaceholderText('Search distributions...')).toBeInTheDocument();
	});
});
