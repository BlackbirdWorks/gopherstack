import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/svelte';
import GlobalAcceleratorPage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getGlobalAcceleratorClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: {
		success: vi.fn(),
		error: vi.fn()
	}
}));

describe('GlobalAccelerator Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title and create button', () => {
		mockSend.mockResolvedValueOnce({ Accelerators: [] });

		render(GlobalAcceleratorPage);

		expect(screen.getByText('Global Accelerator Network')).toBeInTheDocument();
		expect(screen.getByText('Deploy Global IP')).toBeInTheDocument();
	});

	it('displays loaded accelerators', async () => {
		mockSend.mockResolvedValueOnce({
			Accelerators: [
				{ AcceleratorArn: 'arn:aws:ga:us-east-1:123:accelerator/abc', Name: 'prod-accelerator', Status: 'DEPLOYED' },
				{ AcceleratorArn: 'arn:aws:ga:us-east-1:123:accelerator/def', Name: 'dev-accelerator', Status: 'IN_PROGRESS' }
			]
		});

		render(GlobalAcceleratorPage);

		await waitFor(() => {
			expect(screen.getByText('prod-accelerator')).toBeInTheDocument();
		}, { timeout: 3000 });
		expect(screen.getByText('dev-accelerator')).toBeInTheDocument();
	});

	it('filters accelerators via search input', async () => {
		mockSend.mockResolvedValueOnce({
			Accelerators: [
				{ AcceleratorArn: 'arn:1', Name: 'prod-accel', Status: 'DEPLOYED' },
				{ AcceleratorArn: 'arn:2', Name: 'dev-accel', Status: 'DEPLOYED' },
				{ AcceleratorArn: 'arn:3', Name: 'staging-accel', Status: 'DEPLOYED' }
			]
		});

		render(GlobalAcceleratorPage);

		await waitFor(() => {
			expect(screen.getByText('prod-accel')).toBeInTheDocument();
		}, { timeout: 3000 });

		const searchInput = screen.getByPlaceholderText('Search ip groups...');
		await fireEvent.input(searchInput, { target: { value: 'dev' } });

		await waitFor(() => {
			expect(screen.queryByText('prod-accel')).not.toBeInTheDocument();
		});
		expect(screen.getByText('dev-accel')).toBeInTheDocument();
		expect(screen.queryByText('staging-accel')).not.toBeInTheDocument();
	});

	it('opens create modal when button is clicked', async () => {
		mockSend.mockResolvedValueOnce({ Accelerators: [] });

		render(GlobalAcceleratorPage);

		await fireEvent.click(screen.getByText('Deploy Global IP'));

		expect(screen.getByText('Provision Anycast Set')).toBeInTheDocument();
		expect(screen.getByPlaceholderText('e.g. global-entry-prd')).toBeInTheDocument();
	});

	it('closes create modal on abort click', async () => {
		mockSend.mockResolvedValueOnce({ Accelerators: [] });

		render(GlobalAcceleratorPage);

		await fireEvent.click(screen.getByText('Deploy Global IP'));
		expect(screen.getByText('Provision Anycast Set')).toBeInTheDocument();

		await fireEvent.click(screen.getByText('Abort'));

		await waitFor(() => {
			expect(screen.queryByText('Provision Anycast Set')).not.toBeInTheDocument();
		});
	});

	it('creates an accelerator via the modal form', async () => {
		mockSend.mockResolvedValueOnce({ Accelerators: [] });
		mockSend.mockResolvedValueOnce({ Accelerator: { AcceleratorArn: 'arn:3', Name: 'new-accel' } });
		mockSend.mockResolvedValueOnce({ Accelerators: [{ AcceleratorArn: 'arn:3', Name: 'new-accel', Status: 'DEPLOYED' }] });

		render(GlobalAcceleratorPage);

		await fireEvent.click(screen.getByText('Deploy Global IP'));

		const nameInput = screen.getByPlaceholderText('e.g. global-entry-prd');
		await fireEvent.input(nameInput, { target: { value: 'new-accel' } });

		const form = nameInput.closest('form')!;
		await fireEvent.submit(form);

		await waitFor(() => {
			expect(mockSend).toHaveBeenCalledTimes(3);
		}, { timeout: 3000 });

		const { toast } = await import('svelte-sonner');
		expect(toast.success).toHaveBeenCalled();
	});

	it('selects an accelerator and loads details', async () => {
		mockSend.mockResolvedValueOnce({
			Accelerators: [{ AcceleratorArn: 'arn:abc', Name: 'my-accel', Status: 'DEPLOYED' }]
		});
		// selectAccelerator: ListListenersCommand
		mockSend.mockResolvedValueOnce({ Listeners: [] });

		render(GlobalAcceleratorPage);

		await waitFor(() => {
			expect(screen.getByText('my-accel')).toBeInTheDocument();
		}, { timeout: 3000 });

		await fireEvent.click(screen.getByText('my-accel'));

		await waitFor(() => {
			expect(mockSend).toHaveBeenCalledTimes(2);
		}, { timeout: 3000 });
	});

	it('shows empty state when no accelerators exist', async () => {
		mockSend.mockResolvedValueOnce({ Accelerators: [] });

		render(GlobalAcceleratorPage);

		await waitFor(() => {
			expect(screen.getByText('No global anycast IP sets detected.')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('shows error toast on load failure', async () => {
		mockSend.mockRejectedValueOnce(new Error('not authorized'));

		render(GlobalAcceleratorPage);

		const { toast } = await import('svelte-sonner');
		await waitFor(() => {
			expect(vi.mocked(toast.error)).toHaveBeenCalled();
		}, { timeout: 3000 });
	});
});
