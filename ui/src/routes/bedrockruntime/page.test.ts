import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/svelte';
import BedrockRuntimePage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getBedrockRuntimeClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() }
}));

describe('Bedrock Runtime Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValue({ asyncInvokeSummaries: [] });
		render(BedrockRuntimePage);
		expect(screen.getAllByText('Amazon Bedrock Runtime')[0]).toBeInTheDocument();
	});

	it('shows stat cards', () => {
		mockSend.mockResolvedValue({ asyncInvokeSummaries: [] });
		render(BedrockRuntimePage);
		expect(screen.getAllByText('Supported Models')[0]).toBeInTheDocument();
	});

	it('shows Invoke Model section', () => {
		mockSend.mockResolvedValue({ asyncInvokeSummaries: [] });
		render(BedrockRuntimePage);
		expect(screen.getAllByText('Invoke Foundation Model')[0]).toBeInTheDocument();
	});

	it('shows model selector', () => {
		mockSend.mockResolvedValue({ asyncInvokeSummaries: [] });
		render(BedrockRuntimePage);
		expect(screen.getAllByText('Model')[0]).toBeInTheDocument();
	});

	it('shows prompt textarea', () => {
		mockSend.mockResolvedValue({ asyncInvokeSummaries: [] });
		render(BedrockRuntimePage);
		expect(screen.getAllByText('Prompt')[0]).toBeInTheDocument();
	});

	it('shows invoke button', () => {
		mockSend.mockResolvedValue({ asyncInvokeSummaries: [] });
		render(BedrockRuntimePage);
		expect(screen.getAllByText('Invoke Model')[0]).toBeInTheDocument();
	});

	it('shows async invocations tab', () => {
		mockSend.mockResolvedValue({ asyncInvokeSummaries: [] });
		render(BedrockRuntimePage);
		expect(screen.getAllByText('Async Invocations')[0]).toBeInTheDocument();
	});

	it('shows empty async invocations', async () => {
		mockSend.mockResolvedValue({ asyncInvokeSummaries: [] });
		render(BedrockRuntimePage);
		const asyncTab = screen.getByText('Async Invocations');
		asyncTab.click();
		await waitFor(() => {
			expect(screen.getAllByText(/no async invocations/i)[0]).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('shows refresh button', () => {
		mockSend.mockResolvedValue({ asyncInvokeSummaries: [] });
		render(BedrockRuntimePage);
		expect(screen.getByTitle('Refresh')).toBeInTheDocument();
	});
});
