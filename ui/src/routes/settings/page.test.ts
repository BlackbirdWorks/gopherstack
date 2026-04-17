import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/svelte';
import SettingsPage from './+page.svelte';

vi.mock('svelte-sonner', () => ({
	toast: {
		success: vi.fn(),
		error: vi.fn(),
		info: vi.fn(),
		warning: vi.fn()
	}
}));

const mockLocalStorage = (() => {
	let store: Record<string, string> = {};

	return {
		getItem(key: string) {
			return store[key] ?? null;
		},
		setItem(key: string, value: string) {
			store[key] = String(value);
		},
		removeItem(key: string) {
			delete store[key];
		},
		clear() {
			store = {};
		}
	};
})();

Object.defineProperty(window, 'localStorage', { value: mockLocalStorage });

describe('Settings Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		window.localStorage.clear();
	});

	it('renders global settings form by default', () => {
		render(SettingsPage);

		expect(screen.getByText('Settings')).toBeInTheDocument();
		expect(screen.getByText('Global Settings')).toBeInTheDocument();
		expect(screen.getByLabelText('Account ID')).toBeInTheDocument();
		expect(screen.getByLabelText('Default Region')).toBeInTheDocument();
		expect(screen.getByLabelText('IAM Execution')).toBeInTheDocument();
		expect(screen.getByRole('button', { name: 'Save Settings' })).toBeDisabled();
	});

	it('switches to service settings tab and shows service controls', async () => {
		render(SettingsPage);

		const serviceTab = screen.getByRole('button', { name: 'Service Settings' });
		await fireEvent.click(serviceTab);

		expect(screen.getByText('Dashboard Service Settings')).toBeInTheDocument();
		expect(screen.getByLabelText('Auto-refresh Service Tables')).toBeInTheDocument();
		expect(screen.getByLabelText('Refresh Interval (seconds)')).toBeInTheDocument();
		expect(screen.getByLabelText('Max Console Entries Limit')).toBeInTheDocument();
	});

	it('updates and persists settings on save', async () => {
		render(SettingsPage);

		const accountID = screen.getByLabelText('Account ID') as HTMLInputElement;
		await fireEvent.input(accountID, { target: { value: '111122223333' } });

		const saveButton = screen.getByRole('button', { name: 'Save Settings' });
		expect(saveButton).toBeEnabled();
		await fireEvent.click(saveButton);

		const raw = window.localStorage.getItem('gopherstack_settings');
		expect(raw).toBeTruthy();
		const parsed = JSON.parse(raw!);
		expect(parsed.accountID).toBe('111122223333');

		const { toast } = await import('svelte-sonner');
		expect(toast.success).toHaveBeenCalledWith('Settings updated successfully');
	});

	it('resets to defaults when reset is clicked', async () => {
		render(SettingsPage);

		const region = screen.getByLabelText('Default Region') as HTMLInputElement;
		await fireEvent.input(region, { target: { value: 'eu-west-1' } });

		const resetButton = screen.getByRole('button', { name: 'Reset Defaults' });
		await fireEvent.click(resetButton);

		expect((screen.getByLabelText('Default Region') as HTMLInputElement).value).toBe('us-east-1');
	});
});
