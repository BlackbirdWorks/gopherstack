import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor, fireEvent } from '@testing-library/svelte';
import ConsolePage from './+page.svelte';

vi.mock('$lib/api/connect-client', () => {
	return {
		dashboardClient: {
			streamConsole: vi.fn().mockImplementation(async function* () {
				yield {
					request: {
						id: 'req-123',
						timestamp: { seconds: BigInt(Math.floor(Date.now() / 1000)) },
						durationMs: 45,
						method: 'GET',
						path: '/api/v1/test',
						status: 200,
						headers: { 'Content-Type': 'application/json' },
						body: '{"foo": "bar"}'
					}
				};
				await new Promise<void>(resolve => { setTimeout(resolve, 5000); });
			})
		}
	};
});

vi.mock('svelte-sonner', () => ({
	toast: { error: vi.fn(), success: vi.fn() }
}));

describe('Console Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
	});

	it('renders header and initial empty state', () => {
		render(ConsolePage);
		expect(screen.getByText('Live API Console')).toBeInTheDocument();
		expect(screen.getByText('Waiting for requests...')).toBeInTheDocument();
	});

	it('renders streamed requests and shows details on click', async () => {
		render(ConsolePage);

		await waitFor(() => {
			expect(screen.queryByText('Waiting for requests...')).not.toBeInTheDocument();
		}, { timeout: 3000 });

		expect(screen.getByText('GET')).toBeInTheDocument();
		expect(screen.getByText('/api/v1/test')).toBeInTheDocument();
		expect(screen.getByText('200')).toBeInTheDocument();

		// Click row to open drawer
		const reqRow = screen.getByText('/api/v1/test');
		await fireEvent.click(reqRow);

		await waitFor(() => {
			expect(screen.getByText('Request Details')).toBeInTheDocument();
		});
	});
});
