import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/svelte';
import Route53Page from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getRoute53Client: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() }
}));

describe('Route 53 Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValue({ HostedZones: [], IsTruncated: false });
		render(Route53Page);
		expect(screen.getByText('Amazon Route 53')).toBeInTheDocument();
	});

	it('shows Create Hosted Zone button', () => {
		mockSend.mockResolvedValue({ HostedZones: [], IsTruncated: false });
		render(Route53Page);
		expect(screen.getByText('Create Hosted Zone')).toBeInTheDocument();
	});

	it('shows empty state when no zones', async () => {
		mockSend.mockResolvedValue({ HostedZones: [], IsTruncated: false });
		render(Route53Page);
		await waitFor(() => {
			expect(screen.getByText('No hosted zones found')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('displays loaded hosted zones', async () => {
		mockSend.mockResolvedValue({
			HostedZones: [
				{ Id: '/hostedzone/Z123', Name: 'example.com.', Config: { PrivateZone: false, Comment: 'Prod zone' }, ResourceRecordSetCount: 10 },
				{ Id: '/hostedzone/Z456', Name: 'internal.example.com.', Config: { PrivateZone: true, Comment: 'Private' }, ResourceRecordSetCount: 5 }
			],
			IsTruncated: false
		});
		render(Route53Page);
		await waitFor(() => {
			expect(screen.getByText('example.com.')).toBeInTheDocument();
			expect(screen.getByText('internal.example.com.')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('shows Public/Private badges', async () => {
		mockSend.mockResolvedValue({
			HostedZones: [
				{ Id: '/hostedzone/Z123', Name: 'example.com.', Config: { PrivateZone: false }, ResourceRecordSetCount: 5 },
				{ Id: '/hostedzone/Z456', Name: 'internal.com.', Config: { PrivateZone: true }, ResourceRecordSetCount: 3 }
			],
			IsTruncated: false
		});
		render(Route53Page);
		await waitFor(() => {
			expect(screen.getByText('Public')).toBeInTheDocument();
			expect(screen.getByText('Private')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('opens create zone modal', async () => {
		mockSend.mockResolvedValue({ HostedZones: [], IsTruncated: false });
		render(Route53Page);
		await fireEvent.click(screen.getByText('Create Hosted Zone'));
		expect(screen.getByText('Domain Name')).toBeInTheDocument();
	});

	it('cancels create zone modal', async () => {
		mockSend.mockResolvedValue({ HostedZones: [], IsTruncated: false });
		render(Route53Page);
		await fireEvent.click(screen.getByText('Create Hosted Zone'));
		await fireEvent.click(screen.getByText('Cancel'));
		expect(screen.queryByText('Domain Name')).not.toBeInTheDocument();
	});

	it('shows search input', () => {
		mockSend.mockResolvedValue({ HostedZones: [], IsTruncated: false });
		render(Route53Page);
		expect(screen.getByPlaceholderText('Search hosted zones...')).toBeInTheDocument();
	});
});
