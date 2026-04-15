import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/svelte';
import HomePage from './+page.svelte';

describe('Home Page', () => {
	it('renders overview', () => {
		render(HomePage);
		
		expect(screen.getAllByText('GOPHER', { exact: false }).length).toBeGreaterThan(0);
	});
});
