/**
 * Filters module for the FUA Alternance Dashboard.
 * Manages filter state, UI, and events.
 */

const Filters = (() => {
    // Current filter state
    let state = {
        keyword: '',
        technology: '',
        location: '',
        department: '',
        category: '',
        profile: '',
        source: '',
        date_filter: '',
        sort_by: 'date',
        sort_order: 'desc',
        page: 1,
        per_page: 20,
    };

    // Callbacks
    let onChangeCallback = null;

    // Debounce timer
    let debounceTimer = null;

    /**
     * Initialize filter UI and event listeners.
     */
    function init(onChange) {
        onChangeCallback = onChange;
        bindEvents();
    }

    /**
     * Bind all filter-related events.
     */
    function bindEvents() {
        // Search input with debounce
        const searchInput = document.getElementById('searchInput');
        if (searchInput) {
            searchInput.addEventListener('input', (e) => {
                clearTimeout(debounceTimer);
                debounceTimer = setTimeout(() => {
                    state.keyword = e.target.value.trim();
                    state.page = 1;
                    triggerChange();
                }, 400);
            });
        }

        // Select filters
        const selects = {
            filterTechnology: 'technology',
            filterCategory: 'category',
            filterProfile: 'profile',
            filterSource: 'source',
        };

        for (const [id, key] of Object.entries(selects)) {
            const el = document.getElementById(id);
            if (el) {
                el.addEventListener('change', (e) => {
                    state[key] = e.target.value;
                    state.page = 1;
                    triggerChange();
                });
            }
        }

        // Location input with debounce
        const locationInput = document.getElementById('filterLocation');
        if (locationInput) {
            locationInput.addEventListener('input', (e) => {
                clearTimeout(debounceTimer);
                debounceTimer = setTimeout(() => {
                    state.location = e.target.value.trim();
                    state.department = ''; // Clear department when typing a city manually
                    state.page = 1;
                    triggerChange();
                }, 400);
            });
        }

        // Date quick filters
        const dateChips = document.querySelectorAll('.date-chip');
        dateChips.forEach((chip) => {
            chip.addEventListener('click', () => {
                dateChips.forEach((c) => c.classList.remove('active'));
                chip.classList.add('active');
                state.date_filter = chip.dataset.filter;
                state.page = 1;
                triggerChange();
            });
        });

        // Sort select
        const sortSelect = document.getElementById('sortSelect');
        if (sortSelect) {
            sortSelect.addEventListener('change', (e) => {
                const [sortBy, sortOrder] = e.target.value.split('-');
                state.sort_by = sortBy;
                state.sort_order = sortOrder;
                state.page = 1;
                triggerChange();
            });
        }

        // Reset button
        const resetBtn = document.getElementById('btnReset');
        if (resetBtn) {
            resetBtn.addEventListener('click', reset);
        }

        // Mobile sidebar toggle
        const sidebarToggle = document.getElementById('sidebarToggle');
        const sidebar = document.getElementById('sidebar');
        if (sidebarToggle && sidebar) {
            sidebarToggle.addEventListener('click', () => {
                sidebar.classList.toggle('open');
                sidebarToggle.textContent = sidebar.classList.contains('open')
                    ? '✕ Fermer les filtres'
                    : '⚙️ Filtres';
            });
        }
    }

    function formatSource(source) {
        const names = {
            labonnealternance: 'La Bonne Alternance',
            francetravail: 'France Travail',

            linkedin: 'LinkedIn',
            hellowork: 'HelloWork',
            wttj: 'Welcome to the Jungle',
        };
        return names[source] || source;
    }

    /**
     * Populate filter dropdowns with available options.
     */
    function populateOptions(filterData) {
        const mapping = {
            filterTechnology: { data: filterData.technologies || [], label: 'Toutes les technologies' },
            filterCategory: { data: filterData.categories || [], label: 'Tous les secteurs' },
            filterProfile: { data: filterData.profiles, label: 'Tous les profils' },
            filterSource: { data: filterData.sources, label: 'Toutes les sources' },
        };

        for (const [id, config] of Object.entries(mapping)) {
            const select = document.getElementById(id);
            if (!select) continue;

            // Preserve current value
            const currentValue = select.value;

            // Clear and rebuild options
            select.innerHTML = `<option value="">${config.label}</option>`;

            // Special handling for profile to only show bac+2, bac+3, bac+4, bac+5
            if (id === 'filterProfile') {
                const allowedProfiles = ['bac+2', 'bac+3', 'bac+4', 'bac+5'];
                for (const item of allowedProfiles) {
                    const option = document.createElement('option');
                    option.value = item;
                    // Capitalize format
                    option.textContent = item.replace('b', 'B').replace('c+', 'c +');
                    select.appendChild(option);
                }
            } else {
                for (const item of config.data) {
                    if (item) {
                        const option = document.createElement('option');
                        option.value = item;
                        option.textContent = id === 'filterSource' ? formatSource(item) : item;
                        select.appendChild(option);
                    }
                }
            }

            // Restore value if still valid
            if (currentValue) {
                select.value = currentValue;
            }
        }
    }

    /**
     * Reset all filters to defaults.
     * @param {boolean} silent If true, do not trigger a search event.
     */
    function reset(silent = false) {
        state.keyword = '';
        state.technology = '';
        state.location = '';
        state.department = '';
        state.category = '';
        state.profile = '';
        state.source = '';
        state.date_filter = '';
        state.sort_by = 'date';
        state.sort_order = 'desc';
        state.page = 1;
        state.per_page = 20;

        // Reset UI
        const searchEl = document.getElementById('searchInput');
        if (searchEl) searchEl.value = '';

        const techEl = document.getElementById('filterTechnology');
        if (techEl) techEl.value = '';

        const catEl = document.getElementById('filterCategory');
        if (catEl) catEl.value = '';

        const profEl = document.getElementById('filterProfile');
        if (profEl) profEl.value = '';

        const srcEl = document.getElementById('filterSource');
        if (srcEl) srcEl.value = '';

        const locEl = document.getElementById('filterLocation');
        if (locEl) locEl.value = '';

        const sortEl = document.getElementById('sortSelect');
        if (sortEl) sortEl.value = 'date-desc';

        // Reset date chips
        document.querySelectorAll('.date-chip').forEach((c) => c.classList.remove('active'));
        const allChip = document.querySelector('.date-chip[data-filter=""]');
        if (allChip) allChip.classList.add('active');

        if (!silent) {
            triggerChange();
        }
    }

    /**
     * Set the current page.
     */
    function setPage(page) {
        state.page = page;
        triggerChange();
    }

    /**
     * Get current state as API params.
     */
    function getParams() {
        return { ...state };
    }

    /**
     * Trigger the onChange callback.
     */
    function triggerChange() {
        if (onChangeCallback) {
            onChangeCallback(getParams());
        }
    }

    /**
     * Programmatically set a filter value and trigger change.
     * Used by clickable stats to navigate with a pre-filled filter.
     */
    function setFilter(key, value) {
        // Reset all filters first for a clean navigation, but silently!
        reset(true);
        state[key] = value;
        state.page = 1;

        // Update the corresponding UI element
        const uiMapping = {
            technology: 'filterTechnology',
            category: 'filterCategory',
            profile: 'filterProfile',
            source: 'filterSource',
            location: 'filterLocation',
            keyword: 'searchInput',
        };
        const elId = uiMapping[key];
        if (elId) {
            const el = document.getElementById(elId);
            if (el) el.value = value;
        }
        // For department, also show it in the location input
        if (key === 'department') {
            const locEl = document.getElementById('filterLocation');
            if (locEl) locEl.value = value;
        }

        // Only trigger the API call AFTER everything is set
        triggerChange();
    }

    return {
        init,
        populateOptions,
        reset,
        setPage,
        setFilter,
        getParams,
    };
})();
