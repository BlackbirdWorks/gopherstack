import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/svelte';
import AmplifyPage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getAmplifyClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: {
		success: vi.fn(),
		error: vi.fn()
	}
}));

describe('Amplify Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title and create button', () => {
		mockSend.mockResolvedValueOnce({ apps: [] });

		render(AmplifyPage);

		expect(screen.getByText('Amplify Full-Stack')).toBeInTheDocument();
		expect(screen.getByText('Assemble App')).toBeInTheDocument();
	});

	it('displays loaded apps', async () => {
		mockSend.mockResolvedValueOnce({
			apps: [
				{ appId: 'app-1', name: 'my-frontend', platform: 'WEB' },
				{ appId: 'app-2', name: 'admin-panel', platform: 'WEB' }
			]
		});

		render(AmplifyPage);

		await waitFor(() => {
			expect(screen.getByText('my-frontend')).toBeInTheDocument();
		}, { timeout: 3000 });
		expect(screen.getByText('admin-panel')).toBeInTheDocument();
	});

	it('filters apps via search input', async () => {
		mockSend.mockResolvedValueOnce({
			apps: [
				{ appId: 'app-1', name: 'alpha-app', platform: 'WEB' },
				{ appId: 'app-2', name: 'beta-app', platform: 'WEB' },
				{ appId: 'app-3', name: 'gamma-app', platform: 'WEB' }
			]
		});

		render(AmplifyPage);

		await waitFor(() => {
			expect(screen.getByText('alpha-app')).toBeInTheDocument();
		}, { timeout: 3000 });

		const searchInput = screen.getByPlaceholderText('Search applications...');
		await fireEvent.input(searchInput, { target: { value: 'beta' } });

		await waitFor(() => {
			expect(screen.queryByText('alpha-app')).not.toBeInTheDocument();
		});
		expect(screen.getByText('beta-app')).toBeInTheDocument();
		expect(screen.queryByText('gamma-app')).not.toBeInTheDocument();
	});

	it('opens create modal when button is clicked', async () => {
		mockSend.mockResolvedValueOnce({ apps: [] });

		render(AmplifyPage);

		await fireEvent.click(screen.getByText('Assemble App'));

		expect(screen.getByText('Assemble Full-Stack App')).toBeInTheDocument();
		expect(screen.getByPlaceholderText('e.g. gopherstack-frontend-v5')).toBeInTheDocument();
	});

	it('closes create modal on abort click', async () => {
		mockSend.mockResolvedValueOnce({ apps: [] });

		render(AmplifyPage);

		await fireEvent.click(screen.getByText('Assemble App'));
		expect(screen.getByText('Assemble Full-Stack App')).toBeInTheDocument();

		await fireEvent.click(screen.getByText('Abort'));

		await waitFor(() => {
			expect(screen.queryByText('Assemble Full-Stack App')).not.toBeInTheDocument();
		});
	});

	it('creates an app via the modal form', async () => {
		mockSend.mockResolvedValueOnce({ apps: [] });
		mockSend.mockResolvedValueOnce({ app: { appId: 'new-app', name: 'new-frontend' } });
		mockSend.mockResolvedValueOnce({ apps: [{ appId: 'new-app', name: 'new-frontend' }] });

		render(AmplifyPage);

		await fireEvent.click(screen.getByText('Assemble App'));

		const nameInput = screen.getByPlaceholderText('e.g. gopherstack-frontend-v5');
		await fireEvent.input(nameInput, { target: { value: 'new-frontend' } });

		const form = nameInput.closest('form')!;
		await fireEvent.submit(form);

		await waitFor(() => {
			expect(mockSend).toHaveBeenCalledTimes(3);
		}, { timeout: 3000 });

		const { toast } = await import('svelte-sonner');
		expect(toast.success).toHaveBeenCalled();
	});

	it('selects an app and loads branches', async () => {
		mockSend.mockResolvedValueOnce({
			apps: [{ appId: 'app-1', name: 'my-service', platform: 'WEB' }]
		});
		// selectApp: ListBranches
		mockSend.mockResolvedValueOnce({
			branches: [
				{ branchName: 'main', displayName: 'main', stage: 'PRODUCTION' },
				{ branchName: 'dev', displayName: 'dev', stage: 'DEVELOPMENT' }
			]
		});
		// selectBranch: ListJobs
		mockSend.mockResolvedValueOnce({ jobSummaries: [] });

		render(AmplifyPage);

		await waitFor(() => {
			expect(screen.getByText('my-service')).toBeInTheDocument();
		}, { timeout: 3000 });

		await fireEvent.click(screen.getByText('my-service'));

		await waitFor(() => {
			expect(screen.getByText('main')).toBeInTheDocument();
		}, { timeout: 3000 });
		expect(screen.getByText('dev')).toBeInTheDocument();
	});

	it('shows empty state when no apps exist', async () => {
		mockSend.mockResolvedValueOnce({ apps: [] });

		render(AmplifyPage);

		await waitFor(() => {
			expect(screen.getByText('No Amplify apps found.')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('shows error toast on load failure', async () => {
		mockSend.mockRejectedValueOnce(new Error('network error'));

		render(AmplifyPage);

		const { toast } = await import('svelte-sonner');
		await waitFor(() => {
			expect(vi.mocked(toast.error)).toHaveBeenCalled();
		}, { timeout: 3000 });
	});
});
