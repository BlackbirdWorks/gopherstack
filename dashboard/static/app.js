// Gopherstack UI JavaScript — Flowbite + HTMX

// ── Toast notifications ────────────────────────────────────────
function showToast(message, type) {
    type = type || 'info';
    const colors = {
        success: 'bg-emerald-50 text-emerald-800 border-emerald-200 dark:bg-emerald-900/30 dark:text-emerald-400 dark:border-emerald-800/50',
        error: 'bg-red-50 text-red-800 border-red-200 dark:bg-red-900/30 dark:text-red-400 dark:border-red-800/50',
        warning: 'bg-amber-50 text-amber-800 border-amber-200 dark:bg-amber-900/30 dark:text-amber-400 dark:border-amber-800/50',
        info: 'bg-blue-50 text-blue-800 border-blue-200 dark:bg-blue-900/30 dark:text-blue-400 dark:border-blue-800/50',
    };
    const progressColors = {
        success: 'bg-emerald-500',
        error: 'bg-red-500',
        warning: 'bg-amber-500',
        info: 'bg-blue-500',
    };

    // Proper SVG Icons
    const icons = {
        success: `<svg class="w-5 h-5 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>`,
        error: `<svg class="w-5 h-5 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>`,
        warning: `<svg class="w-5 h-5 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"></path></svg>`,
        info: `<svg class="w-5 h-5 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>`,
    };

    const colorClass = colors[type] || colors.info;
    const progressColor = progressColors[type] || progressColors.info;
    const icon = icons[type] || icons.info;

    const toast = document.createElement('div');
    toast.className = `flex flex-col relative overflow-hidden rounded-lg shadow-lg border pointer-events-auto transition-all duration-300 transform opacity-0 translate-y-2 ${colorClass} w-80`;

    toast.innerHTML = `
        <div class="flex items-start gap-3 p-4">
            ${icon}
            <div class="flex-1 text-sm font-medium pt-0.5">${message}</div>
            <button class="flex-shrink-0 text-current opacity-50 hover:opacity-100 focus:outline-none" onclick="this.closest('.flex-col').remove()">
                <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path></svg>
            </button>
        </div>
        <div class="h-1 w-full bg-black/10 dark:bg-white/10 relative">
            <div class="absolute top-0 left-0 h-full ${progressColor} transition-all duration-[5000ms] ease-linear w-full" id="progress-${Date.now()}"></div>
        </div>
    `;

    document.getElementById('global-alerts').appendChild(toast);

    // Initial animation
    requestAnimationFrame(function () {
        toast.classList.remove('opacity-0', 'translate-y-2');
        toast.classList.add('opacity-100', 'translate-y-0');
        // Start progress bar
        setTimeout(() => {
            const bar = toast.querySelector('div[id^="progress-"]');
            if (bar) {
                bar.style.width = '0%';
            }
        }, 50);
    });

    // Auto dismiss
    setTimeout(function () {
        if (!document.body.contains(toast)) return;
        toast.classList.add('opacity-0', '-translate-x-2');
        setTimeout(function () { if (document.body.contains(toast)) toast.remove(); }, 300);
    }, 5000);
}

// ── Copy to clipboard ──────────────────────────────────────────
function copyToClipboard(text) {
    navigator.clipboard.writeText(text).then(function () {
        showToast('Copied to clipboard!', 'success');
    }).catch(function (err) {
        showToast('Failed to copy', 'error');
        console.error('Copy failed:', err);
    });
}

// ── Format JSON ────────────────────────────────────────────────
function formatJSON(obj) {
    return JSON.stringify(obj, null, 2);
}

// ── Global confirm modal (Flowbite) ───────────────────────────
function confirmDelete(message) {
    return new Promise(function (resolve) {
        const modalEl = document.getElementById('global_confirm_modal');
        const confirmBtn = document.getElementById('global_confirm_proceed');
        const cancelBtn = document.getElementById('global_confirm_cancel');
        const msgEl = document.getElementById('global_confirm_message');
        msgEl.textContent = message || 'Are you sure you want to delete this?';

        // Using Flowbite Modal API if available, otherwise fallback to hidden toggle
        let modal = null;
        if (window.Modal) {
            modal = new Modal(modalEl);
        }

        const show = () => {
            if (modal) modal.show();
            else modalEl.classList.remove('hidden');
        };
        const hide = () => {
            if (modal) modal.hide();
            else modalEl.classList.add('hidden');
        };

        const onConfirm = function () { hide(); cleanup(); resolve(true); };
        const onCancel = function () { hide(); cleanup(); resolve(false); };
        const cleanup = function () {
            confirmBtn.removeEventListener('click', onConfirm);
            cancelBtn.removeEventListener('click', onCancel);
        };
        confirmBtn.addEventListener('click', onConfirm);
        cancelBtn.addEventListener('click', onCancel);
        show();
    });
}

// ── HTMX: intercept hx-confirm ────────────────────────────────
document.addEventListener('htmx:confirm', function (event) {
    if (!event.detail.question) return;
    event.preventDefault();

    const modalEl = document.getElementById('global_confirm_modal');
    const confirmBtn = document.getElementById('global_confirm_proceed');
    const cancelBtn = document.getElementById('global_confirm_cancel');
    const msgEl = document.getElementById('global_confirm_message');
    msgEl.textContent = event.detail.question;

    let modal = null;
    if (window.Modal) {
        modal = new Modal(modalEl);
    }

    const show = () => {
        if (modal) modal.show();
        else modalEl.classList.remove('hidden');
    };
    const hide = () => {
        if (modal) modal.hide();
        else modalEl.classList.add('hidden');
    };

    const handleConfirm = function () { hide(); cleanup(); event.detail.issueRequest(true); };
    const handleCancel = function () { hide(); cleanup(); };
    const cleanup = function () {
        confirmBtn.removeEventListener('click', handleConfirm);
        cancelBtn.removeEventListener('click', handleCancel);
    };
    confirmBtn.addEventListener('click', handleConfirm);
    cancelBtn.addEventListener('click', handleCancel);
    show();
});

// ── Sidebar Manager ───────────────────────────────────────────
window.SidebarManager = {
    init: function () {
        // Init mini mode
        if (localStorage.getItem('gopherstack-sidebar-mini') === 'true') {
            document.body.classList.add('sidebar-mini');
        }

        // Init collapsible sections
        try {
            const collapsed = JSON.parse(localStorage.getItem('gopherstack-sidebar-sections') || '[]');
            collapsed.forEach(id => {
                const section = document.getElementById(`section-${id}`);
                const chevron = document.getElementById(`chevron-${id}`);
                if (section) {
                    section.classList.add('max-h-0', 'opacity-0');
                    if (chevron) chevron.classList.add('-rotate-90');
                }
            });
        } catch (e) {
            console.error("Failed to parse sidebar state", e);
        }
    },
    toggleMiniMode: function () {
        document.body.classList.toggle('sidebar-mini');
        const isMini = document.body.classList.contains('sidebar-mini');
        localStorage.setItem('gopherstack-sidebar-mini', isMini ? 'true' : 'false');
    },
    toggleSection: function (id) {
        const section = document.getElementById(`section-${id}`);
        const chevron = document.getElementById(`chevron-${id}`);
        if (!section) return;

        const isCollapsed = section.classList.contains('max-h-0');

        if (isCollapsed) {
            section.classList.remove('max-h-0', 'opacity-0');
            if (chevron) chevron.classList.remove('-rotate-90');
        } else {
            section.classList.add('max-h-0', 'opacity-0');
            if (chevron) chevron.classList.add('-rotate-90');
        }

        // Save state
        try {
            let collapsed = JSON.parse(localStorage.getItem('gopherstack-sidebar-sections') || '[]');
            if (!isCollapsed && !collapsed.includes(id)) {
                collapsed.push(id);
            } else if (isCollapsed) {
                collapsed = collapsed.filter(c => c !== id);
            }
            localStorage.setItem('gopherstack-sidebar-sections', JSON.stringify(collapsed));
        } catch (e) { }
    }
};

// ── Table Manager ─────────────────────────────────────────────
window.TableManager = {
    init: function () {
        if (localStorage.getItem('gopherstack-table-compact') === 'true') {
            document.body.classList.add('table-compact');
        }
    },
    toggleCompactMode: function () {
        document.body.classList.toggle('table-compact');
        const isCompact = document.body.classList.contains('table-compact');
        localStorage.setItem('gopherstack-table-compact', isCompact ? 'true' : 'false');
    }
};

// ── Theme Manager ─────────────────────────────────────────────
window.ThemeManager = {
    init: function () {
        const darkIcon = document.getElementById('theme-icon-dark');
        const lightIcon = document.getElementById('theme-icon-light');
        if (!darkIcon || !lightIcon) return;

        // Change the icons inside the button based on current settings
        if (document.documentElement.classList.contains('dark')) {
            lightIcon.classList.add('hidden');
            darkIcon.classList.remove('hidden');
        } else {
            darkIcon.classList.add('hidden');
            lightIcon.classList.remove('hidden');
        }
    },
    toggle: function () {
        const darkIcon = document.getElementById('theme-icon-dark');
        const lightIcon = document.getElementById('theme-icon-light');

        if (darkIcon && lightIcon) {
            darkIcon.classList.toggle('hidden');
            lightIcon.classList.toggle('hidden');
        }

        if (localStorage.getItem('gopherstack-theme')) {
            if (localStorage.getItem('gopherstack-theme') === 'light') {
                document.documentElement.classList.add('dark');
                localStorage.setItem('gopherstack-theme', 'dark');
            } else {
                document.documentElement.classList.remove('dark');
                localStorage.setItem('gopherstack-theme', 'light');
            }
        } else {
            if (document.documentElement.classList.contains('dark')) {
                document.documentElement.classList.remove('dark');
                localStorage.setItem('gopherstack-theme', 'light');
            } else {
                document.documentElement.classList.add('dark');
                localStorage.setItem('gopherstack-theme', 'dark');
            }
        }
    }
};

// ── Sparkline Heatmap ───────────────────────────────────────────
window.SparklineManager = {
    intervalId: null,
    operationSpike: false,
    init: function () {
        const container = document.getElementById('activity-sparkline');
        if (!container) return; // not on metrics page

        // Clear children and existing interval if any
        container.innerHTML = '';
        if (this.intervalId) clearInterval(this.intervalId);

        const numBars = 12;
        for (let i = 0; i < numBars; i++) {
            const bar = document.createElement('div');
            bar.className = 'w-1.5 bg-indigo-500/80 dark:bg-indigo-400/80 rounded-t-[1px] transition-all duration-300 ease-out';
            bar.style.height = (10 + Math.random() * 40) + '%';
            container.appendChild(bar);
        }

        this.intervalId = setInterval(() => {
            const bars = container.children;
            if (bars.length === 0) return;
            // Shift values left
            for (let i = 0; i < bars.length - 1; i++) {
                bars[i].style.height = bars[i + 1].style.height;
            }
            // Generate next height
            let nextH = 10 + Math.random() * 30; // Baseline noise

            // Random artificial or real spikes
            if (this.operationSpike || Math.random() > 0.85) {
                nextH += 40 + Math.random() * 30; // Spike
                this.operationSpike = false;
            }

            if (nextH > 100) nextH = 100;
            bars[bars.length - 1].style.height = nextH + '%';
        }, 800);
    },
    triggerSpike: function () {
        this.operationSpike = true;
    }
};

window.addEventListener('metrics-updated', () => {
    if (window.SparklineManager) window.SparklineManager.triggerSpike();
});

// ── Sidebar Scroll Preservation ───────────────────────────────
window.lastSidebarScroll = window.lastSidebarScroll || 0;

document.addEventListener('htmx:beforeRequest', function () {
    const sidebarList = document.querySelector('#sidebar .overflow-y-auto');
    if (sidebarList) {
        window.lastSidebarScroll = sidebarList.scrollTop;
    }
});

// ── HTMX Lifecycle Hooks ─────────────────────────────────────────────
document.addEventListener('htmx:beforeSwap', function (e) {
    // Clean up any modals that were moved to the body to prevent duplicates
    document.querySelectorAll('body > div[id^="snippetModal-"]').forEach(modal => {
        modal.remove();
    });
    document.body.style.overflow = '';
});

document.addEventListener('htmx:afterSwap', function () {
    console.log('HTMX swap completed');
    const sidebarList = document.querySelector('#sidebar .overflow-y-auto');
    if (sidebarList) {
        sidebarList.scrollTop = window.lastSidebarScroll || 0;
    }
    setupGlobalSearch();

    if (window.ThemeManager) {
        window.ThemeManager.init();
    }
    if (window.SidebarManager) window.SidebarManager.init();
    if (window.TableManager) window.TableManager.init();

    if (typeof window.initSnippets === 'function') {
        window.initSnippets();
    }

    if (window.SparklineManager) {
        window.SparklineManager.init();
    }

    // Re-initialize all Flowbite components (modals, dropdowns) after DOM replaces
    if (typeof window.initFlowbite === 'function') {
        window.initFlowbite();
    } else if (typeof window.initModals === 'function') {
        window.initModals();
    }
});

// ── Snippet Modal Functions ───────────────────────────────────
window.initSnippets = function () {
    // Setup observer to highlight code when modal is opened since Prism might skip hidden elements
    const observer = new MutationObserver((mutations) => {
        mutations.forEach((mutation) => {
            if (mutation.attributeName === 'aria-hidden' && mutation.target.getAttribute('aria-hidden') === 'false') {
                if (window.Prism) {
                    const codeBlocks = mutation.target.querySelectorAll('code[class*="language-"]');
                    codeBlocks.forEach(block => window.Prism.highlightElement(block));
                }
            }
        });
    });

    document.querySelectorAll('[id^="snippetModal-"]').forEach(modal => {
        observer.observe(modal, { attributes: true });
    });
};

window.switchSnippet = function (btn, targetId) {
    const container = btn.closest('.code-snippet-generator');

    // Update tabs
    const tabs = container.querySelectorAll('.snippet-tab');
    tabs.forEach(t => {
        t.classList.remove('active', 'bg-emerald-600', 'text-white', 'shadow-sm', 'hover:bg-emerald-700');
        t.classList.add('text-slate-600', 'hover:text-slate-900', 'dark:text-slate-400', 'dark:hover:text-white', 'hover:bg-slate-200', 'dark:hover:bg-slate-800');
    });
    btn.classList.add('active', 'bg-emerald-600', 'text-white', 'shadow-sm', 'hover:bg-emerald-700');
    btn.classList.remove('text-slate-600', 'hover:text-slate-900', 'dark:text-slate-400', 'dark:hover:text-white', 'hover:bg-slate-200', 'dark:hover:bg-slate-800');

    // Update content with fade transition
    container.querySelectorAll('.snippet-content').forEach(s => {
        if (!s.classList.contains('hidden')) {
            s.classList.add('opacity-0', 'scale-95');
            setTimeout(() => {
                s.classList.add('hidden');
                s.classList.remove('opacity-0', 'scale-95');
            }, 150);
        }
    });

    const target = document.getElementById(targetId);
    if (target) {
        setTimeout(() => {
            target.classList.remove('hidden');
            target.classList.add('opacity-0', 'scale-95');
            // Trigger reflow
            void target.offsetWidth;
            target.classList.remove('opacity-0', 'scale-95');
            target.classList.add('transition-all', 'duration-200');

            // Re-highlight if visible
            if (window.Prism) {
                const codeBlock = target.querySelector('code');
                if (codeBlock) window.Prism.highlightElement(codeBlock);
            }
        }, 150);
    }
};

window.copyActiveSnippet = function (btn) {
    const container = btn.closest('.relative');
    const activeContent = Array.from(container.querySelectorAll('.snippet-content')).find(el => !el.classList.contains('hidden'));

    if (activeContent) {
        const textToCopy = activeContent.innerText.replace("Copied", "").trim();

        navigator.clipboard.writeText(textToCopy).then(() => {
            showToast('Code copied to clipboard!', 'success');

            const copyIcon = btn.querySelector('.copy-icon');
            const checkIcon = btn.querySelector('.check-icon');

            if (copyIcon) copyIcon.classList.add('hidden');
            if (checkIcon) checkIcon.classList.remove('hidden', 'opacity-0');

            btn.classList.add('!bg-emerald-600/90', '!border-emerald-500');

            setTimeout(() => {
                if (copyIcon) copyIcon.classList.remove('hidden');
                if (checkIcon) checkIcon.classList.add('hidden', 'opacity-0');
                btn.classList.remove('!bg-emerald-600/90', '!border-emerald-500');
            }, 2000);
        }).catch(err => {
            showToast('Failed to copy', 'error');
            console.error('Copy failed:', err);
        });
    }
};

window.openSnippetModal = function (id) {
    const modal = document.getElementById("snippetModal-" + id);
    if (modal) {
        if (modal.parentElement !== document.body) {
            document.body.appendChild(modal);
        }
        modal.classList.remove('hidden');
        modal.classList.add('flex');
        modal.setAttribute('aria-hidden', 'false');
        document.body.style.overflow = 'hidden';
    }
};

window.closeSnippetModal = function (id) {
    const modal = document.getElementById("snippetModal-" + id);
    if (modal) {
        modal.classList.add('hidden');
        modal.classList.remove('flex');
        modal.setAttribute('aria-hidden', 'true');
        document.body.style.overflow = '';
    }
};

// ── Global Search ─────────────────────────────────────────────
function setupGlobalSearch() {
    const searchInput = document.getElementById('global-search');
    if (!searchInput) return;

    // Remove existing listener to avoid duplicates on htmx swaps
    searchInput.replaceWith(searchInput.cloneNode(true));
    const newSearchInput = document.getElementById('global-search');

    newSearchInput.addEventListener('input', function (e) {
        const query = e.target.value.toLowerCase();
        const sidebarItems = document.querySelectorAll('#sidebar li');

        let currentHeader = null;
        let anyVisibleInSection = false;

        sidebarItems.forEach(item => {
            if (item.classList.contains('text-xs')) { // This is a section header
                if (currentHeader) {
                    currentHeader.style.display = anyVisibleInSection ? '' : 'none';
                }
                currentHeader = item;
                anyVisibleInSection = false;
            } else {
                const a = item.querySelector('a');
                if (a) {
                    const text = a.textContent.toLowerCase();
                    if (text.includes(query)) {
                        item.style.display = '';
                        anyVisibleInSection = true;
                    } else {
                        item.style.display = 'none';
                    }
                }
            }
        });

        // Handle the last section
        if (currentHeader) {
            currentHeader.style.display = anyVisibleInSection ? '' : 'none';
        }
    });
}

document.addEventListener('keydown', function (e) {
    if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
        const searchInput = document.getElementById('global-search');
        if (searchInput) {
            e.preventDefault();
            searchInput.focus();
        }
    }
});

document.addEventListener('DOMContentLoaded', function () {
    if (window.ThemeManager) window.ThemeManager.init();
    if (window.SidebarManager) window.SidebarManager.init();
    if (window.TableManager) window.TableManager.init();
    if (window.initSnippets) window.initSnippets();
    if (window.SparklineManager) window.SparklineManager.init();
    setupGlobalSearch();
    console.log('Gopherstack UI loaded');
});
