import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/svelte';
import ACMPCAPage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getACMPCAClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: {
		success: vi.fn(),
		error: vi.fn()
	}
}));

describe('ACM PCA Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title and provision button', () => {
		mockSend.mockResolvedValueOnce({ CertificateAuthorities: [] });

		render(ACMPCAPage);

		expect(screen.getByText('Private CA Registry')).toBeInTheDocument();
		expect(screen.getByText('Provision CA')).toBeInTheDocument();
	});

	it('displays loaded certificate authorities', async () => {
		mockSend.mockResolvedValueOnce({
			CertificateAuthorities: [
				{
					Arn: 'arn:aws:acm-pca:us-east-1:123:ca/abc-1',
					Status: 'ACTIVE',
					CertificateAuthorityConfiguration: { Subject: { CommonName: 'root-ca' } }
				},
				{
					Arn: 'arn:aws:acm-pca:us-east-1:123:ca/abc-2',
					Status: 'DISABLED',
					CertificateAuthorityConfiguration: { Subject: { CommonName: 'sub-ca' } }
				}
			]
		});

		render(ACMPCAPage);

		await waitFor(() => {
			expect(screen.getByText('root-ca')).toBeInTheDocument();
		}, { timeout: 3000 });
		expect(screen.getByText('sub-ca')).toBeInTheDocument();
	});

	it('filters CAs via search input', async () => {
		mockSend.mockResolvedValueOnce({
			CertificateAuthorities: [
				{
					Arn: 'arn:aws:acm-pca:us-east-1:123:ca/a',
					Status: 'ACTIVE',
					CertificateAuthorityConfiguration: { Subject: { CommonName: 'internal-ca' } }
				},
				{
					Arn: 'arn:aws:acm-pca:us-east-1:123:ca/b',
					Status: 'ACTIVE',
					CertificateAuthorityConfiguration: { Subject: { CommonName: 'external-ca' } }
				}
			]
		});

		render(ACMPCAPage);

		await waitFor(() => {
			expect(screen.getByText('internal-ca')).toBeInTheDocument();
		}, { timeout: 3000 });

		const searchInput = screen.getByPlaceholderText('Search CAs...');
		await fireEvent.input(searchInput, { target: { value: 'internal' } });

		await waitFor(() => {
			expect(screen.queryByText('external-ca')).not.toBeInTheDocument();
		});
		expect(screen.getByText('internal-ca')).toBeInTheDocument();
	});

	it('opens create modal when provision button is clicked', async () => {
		mockSend.mockResolvedValueOnce({ CertificateAuthorities: [] });

		render(ACMPCAPage);

		await fireEvent.click(screen.getByText('Provision CA'));

		expect(screen.getByText('Provision Private CA')).toBeInTheDocument();
		expect(screen.getByPlaceholderText('e.g. gopherstack-internal-pki')).toBeInTheDocument();
	});

	it('closes create modal on abort click', async () => {
		mockSend.mockResolvedValueOnce({ CertificateAuthorities: [] });

		render(ACMPCAPage);

		await fireEvent.click(screen.getByText('Provision CA'));
		expect(screen.getByText('Provision Private CA')).toBeInTheDocument();

		await fireEvent.click(screen.getByText('Abort'));

		await waitFor(() => {
			expect(screen.queryByText('Provision Private CA')).not.toBeInTheDocument();
		});
	});

	it('creates a CA via the modal form', async () => {
		mockSend.mockResolvedValueOnce({ CertificateAuthorities: [] });
		mockSend.mockResolvedValueOnce({ CertificateAuthorityArn: 'arn:new' });
		mockSend.mockResolvedValueOnce({ CertificateAuthorities: [{ Arn: 'arn:new', Status: 'CREATING' }] });

		render(ACMPCAPage);

		await fireEvent.click(screen.getByText('Provision CA'));

		const nameInput = screen.getByPlaceholderText('e.g. gopherstack-internal-pki');
		await fireEvent.input(nameInput, { target: { value: 'my-root-ca' } });

		const form = nameInput.closest('form')!;
		await fireEvent.submit(form);

		await waitFor(() => {
			expect(mockSend).toHaveBeenCalledTimes(3);
		}, { timeout: 3000 });

		const { toast } = await import('svelte-sonner');
		expect(toast.success).toHaveBeenCalled();
	});

	it('selects a CA and shows details', async () => {
		mockSend.mockResolvedValueOnce({
			CertificateAuthorities: [
				{
					Arn: 'arn:aws:acm-pca:us-east-1:123:ca/abc',
					Status: 'ACTIVE',
					CertificateAuthorityConfiguration: { Subject: { CommonName: 'my-ca' } }
				}
			]
		});
		mockSend.mockResolvedValueOnce({
			CertificateAuthority: {
				Arn: 'arn:aws:acm-pca:us-east-1:123:ca/abc',
				Status: 'ACTIVE',
				Type: 'ROOT',
				CertificateAuthorityConfiguration: { Subject: { CommonName: 'my-ca' } }
			}
		});

		render(ACMPCAPage);

		await waitFor(() => {
			expect(screen.getByText('my-ca')).toBeInTheDocument();
		}, { timeout: 3000 });

		await fireEvent.click(screen.getByText('my-ca'));

		await waitFor(() => {
			expect(screen.getByTitle('Purge Authority')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('shows empty state when no CAs exist', async () => {
		mockSend.mockResolvedValueOnce({ CertificateAuthorities: [] });

		render(ACMPCAPage);

		await waitFor(() => {
			expect(screen.getByText('No Private CAs tracked.')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('shows error toast on load failure', async () => {
		mockSend.mockRejectedValueOnce(new Error('access denied'));

		render(ACMPCAPage);

		const { toast } = await import('svelte-sonner');
		await waitFor(() => {
			expect(vi.mocked(toast.error)).toHaveBeenCalled();
		}, { timeout: 3000 });
	});
});
