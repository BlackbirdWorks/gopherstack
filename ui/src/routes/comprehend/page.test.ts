import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/svelte';
import ComprehendPage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getComprehendClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() }
}));

describe('Comprehend Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValue({ DocumentClassifierPropertiesList: [], EntityRecognizerPropertiesList: [], TopicsDetectionJobPropertiesList: [] });
		render(ComprehendPage);
		expect(screen.getAllByText('Amazon Comprehend')[0]).toBeInTheDocument();
	});

	it('shows stat cards', () => {
		mockSend.mockResolvedValue({ DocumentClassifierPropertiesList: [] });
		render(ComprehendPage);
		expect(screen.getAllByText('Document Classifiers')[0]).toBeInTheDocument();
	});

	it('shows search input', () => {
		mockSend.mockResolvedValue({ DocumentClassifierPropertiesList: [] });
		render(ComprehendPage);
		expect(screen.getByPlaceholderText(/search/i)).toBeInTheDocument();
	});

	it('shows empty state when no classifiers', async () => {
		mockSend.mockResolvedValue({ DocumentClassifierPropertiesList: [], EntityRecognizerPropertiesList: [], TopicsDetectionJobPropertiesList: [] });
		render(ComprehendPage);
		await waitFor(() => {
			expect(screen.getAllByText(/no document classifiers/i)[0]).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('shows refresh button', () => {
		mockSend.mockResolvedValue({ DocumentClassifierPropertiesList: [] });
		render(ComprehendPage);
		expect(screen.getByTitle('Refresh')).toBeInTheDocument();
	});

	it('shows Entity Recognizers tab', () => {
		mockSend.mockResolvedValue({ DocumentClassifierPropertiesList: [] });
		render(ComprehendPage);
		expect(screen.getAllByText('Entity Recognizers')[0]).toBeInTheDocument();
	});

	it('shows Topics Jobs tab', () => {
		mockSend.mockResolvedValue({ DocumentClassifierPropertiesList: [] });
		render(ComprehendPage);
		expect(screen.getAllByText('Topics Jobs')[0]).toBeInTheDocument();
	});

	it('shows Entity Recognizers stat', () => {
		mockSend.mockResolvedValue({ DocumentClassifierPropertiesList: [] });
		render(ComprehendPage);
		expect(screen.getAllByText('Entity Recognizers').length).toBeGreaterThanOrEqual(1);
	});
});
