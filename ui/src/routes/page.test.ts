import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/svelte';
import HomePage from './+page.svelte';

describe('Home Page', () => {
	it('renders overview', () => {
		render(HomePage);
		
		expect(screen.getByText('Dashboard 2 Migration Workspace')).toBeInTheDocument();
		expect(screen.getByText('Overview')).toBeInTheDocument();
	});
});
