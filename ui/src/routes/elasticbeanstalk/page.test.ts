import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/svelte';
import ElasticBeanstalkPage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getElasticBeanstalkClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() }
}));

describe('Elastic Beanstalk Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValue({ Applications: [], Environments: [] });
		render(ElasticBeanstalkPage);
		expect(screen.getByText('AWS Elastic Beanstalk')).toBeInTheDocument();
	});

	it('shows stat cards', () => {
		mockSend.mockResolvedValue({ Applications: [] });
		render(ElasticBeanstalkPage);
		expect(screen.getByText('Applications')).toBeInTheDocument();
	});

	it('shows search input', () => {
		mockSend.mockResolvedValue({ Applications: [] });
		render(ElasticBeanstalkPage);
		expect(screen.getByPlaceholderText(/search/i)).toBeInTheDocument();
	});

	it('shows empty state when no applications', async () => {
		mockSend.mockResolvedValue({ Applications: [], Environments: [] });
		render(ElasticBeanstalkPage);
		await waitFor(() => {
			expect(screen.getByText(/no applications/i)).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('displays loaded applications', async () => {
		mockSend.mockResolvedValue({
			Applications: [{ ApplicationName: 'my-app', Description: 'My web app', Versions: ['v1', 'v2'] }],
			Environments: []
		});
		render(ElasticBeanstalkPage);
		await waitFor(() => {
			expect(screen.getByText('my-app')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('shows refresh button', () => {
		mockSend.mockResolvedValue({ Applications: [] });
		render(ElasticBeanstalkPage);
		expect(screen.getByTitle('Refresh')).toBeInTheDocument();
	});

	it('shows Environments tab', () => {
		mockSend.mockResolvedValue({ Applications: [] });
		render(ElasticBeanstalkPage);
		expect(screen.getByText('Environments')).toBeInTheDocument();
	});

	it('shows Ready stat', () => {
		mockSend.mockResolvedValue({ Applications: [] });
		render(ElasticBeanstalkPage);
		expect(screen.getByText('Ready')).toBeInTheDocument();
	});
});
