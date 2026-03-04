/**
 * Main application for the FUA Alternance Dashboard.
 * Orchestrates API calls, rendering, user interactions, favorites, and statistics.
 */

document.addEventListener('DOMContentLoaded', () => {
    // State
    let currentModalOffer = null;
    let currentTimelineScale = 'month';
    let cachedTimelineData = {};
    let cachedTechStats = null;
    let cachedGeneralStats = null;

    // ===== INITIALIZATION =====
    initTheme();
    initUserUI();
    Filters.init(handleFilterChange);
    loadFilters();
    loadOffers();
    initTabs();
    initModal();
    initFavFilters();
    initTimelineControls();

    const btnScrape = document.getElementById('btnScrape');
    if (btnScrape) btnScrape.addEventListener('click', handleScrape);
    checkScrapeStatus(); // Check if scrape already running in background

    // Background preloading of stats for instant access
    setTimeout(() => {
        loadTechStats(true); // silent = true
    }, 500);

    // Initial indicator position
    setTimeout(updateTabIndicator, 100);
    window.addEventListener('resize', updateTabIndicator);

    const btnReset = document.getElementById('btnReset');
    if (btnReset) {
        btnReset.addEventListener('click', () => {
            Filters.reset();
        });
    }

    const headerLogo = document.getElementById('headerLogo');
    if (headerLogo) {
        headerLogo.addEventListener('click', () => {
            const tabBtns = document.querySelectorAll('.tab-btn');
            tabBtns.forEach((b) => b.classList.remove('active'));
            const offersTab = document.getElementById('tabOffers');
            if (offersTab) offersTab.classList.add('active');

            const tabsContainer = document.querySelector('.header-tabs');
            if (tabsContainer) tabsContainer.dataset.activeTab = 'offers';

            updateTabIndicator();

            document.getElementById('contentOffers').classList.remove('hidden');
            document.getElementById('contentStats').classList.add('hidden');
            document.getElementById('contentFavorites').classList.add('hidden');

            const sidebar = document.querySelector('.sidebar');
            if (sidebar) sidebar.classList.remove('hidden');

            Filters.reset(false);
            window.scrollTo({ top: 0, behavior: 'smooth' });
        });
    }

    // ===== THEME =====
    function initTheme() {
        const saved = localStorage.getItem('fua-theme') || 'dark';
        document.documentElement.setAttribute('data-theme', saved);
        const btn = document.getElementById('btnThemeToggle');
        if (btn) {
            btn.addEventListener('click', () => {
                const current = document.documentElement.getAttribute('data-theme');
                const next = current === 'dark' ? 'light' : 'dark';
                document.documentElement.setAttribute('data-theme', next);
                localStorage.setItem('fua-theme', next);
            });
        }
    }

    // ===== USER UI =====
    function initUserUI() {
        const slot = document.getElementById('userSlot');
        if (!slot) return;
        renderUserSlot(slot);
    }

    function renderUserSlot(slot) {
        if (API.isLoggedIn()) {
            const user = API.getUser();
            const initial = user?.username?.charAt(0).toUpperCase() || '?';
            slot.innerHTML = `
                <div class="user-menu">
                    <button class="btn-user" id="btnUser">
                        <span class="user-avatar">${initial}</span>
                        ${escapeHtml(user?.username || '')}
                    </button>
                    <div class="user-dropdown" id="userDropdown">
                        <button class="user-dropdown-item" disabled>${escapeHtml(user?.email || '')}</button>
                        <button class="user-dropdown-item logout" id="btnLogout">Déconnexion</button>
                    </div>
                </div>`;
            document.getElementById('btnUser').addEventListener('click', () => {
                document.getElementById('userDropdown').classList.toggle('open');
            });
            document.getElementById('btnLogout').addEventListener('click', () => {
                API.logout();
                window.location.reload();
            });
            // Close dropdown on outside click
            document.addEventListener('click', (e) => {
                const dd = document.getElementById('userDropdown');
                if (dd && !e.target.closest('.user-menu')) dd.classList.remove('open');
            });
        } else {
            slot.innerHTML = `<a href="/auth.html" class="btn-login">Connexion</a>`;
        }
    }

    // ===== TAB NAVIGATION =====
    function initTabs() {
        const tabBtns = document.querySelectorAll('.tab-btn');
        tabBtns.forEach((btn) => {
            btn.addEventListener('click', () => {
                tabBtns.forEach((b) => b.classList.remove('active'));
                btn.classList.add('active');

                const tab = btn.dataset.tab;
                document.getElementById('contentOffers').classList.add('hidden');
                document.getElementById('contentStats').classList.add('hidden');
                document.getElementById('contentFavorites').classList.add('hidden');

                const sidebar = document.querySelector('.sidebar');

                if (tab === 'offers') {
                    document.getElementById('contentOffers').classList.remove('hidden');
                    if (sidebar) sidebar.classList.remove('hidden');
                } else if (tab === 'stats') {
                    document.getElementById('contentStats').classList.remove('hidden');
                    if (sidebar) sidebar.classList.add('hidden');
                    loadTechStats();
                } else if (tab === 'favorites') {
                    document.getElementById('contentFavorites').classList.remove('hidden');
                    if (sidebar) sidebar.classList.add('hidden');
                    loadFavorites();
                }

                const tabsContainer = document.querySelector('.header-tabs');
                if (tabsContainer) tabsContainer.dataset.activeTab = tab;

                updateTabIndicator();
            });
        });
    }

    function updateTabIndicator() {
        const activeBtn = document.querySelector('.tab-btn.active');
        const indicator = document.getElementById('tabIndicator');
        if (!activeBtn || !indicator) return;

        indicator.style.width = `${activeBtn.offsetWidth}px`;
        indicator.style.left = `${activeBtn.offsetLeft}px`;
    }

    // ===== MODAL =====
    function initModal() {
        const overlay = document.getElementById('modalOverlay');
        const close = document.getElementById('modalClose');
        if (close) close.addEventListener('click', closeModal);
        if (overlay) {
            overlay.addEventListener('click', (e) => {
                if (e.target === overlay) closeModal();
            });
        }
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') closeModal();
        });
    }

    function refreshCurrentView() {
        const activeTab = document.querySelector('.tab-btn.active')?.dataset.tab;
        if (activeTab === 'offers') loadOffers();
        if (activeTab === 'favorites') loadFavorites();
    }

    // ===== HANDLERS =====
    async function handleFilterChange(params) {
        await loadOffers(params);
    }


    async function checkScrapeStatus() {
        try {
            const status = await API.getScrapeStatus();
            const btn = document.getElementById('btnScrape');
            const spinnerFill = document.getElementById('scrapeSpinnerFill');

            if (status.is_running) {
                btn.disabled = true;
                btn.classList.add('is-scraping');
                if (spinnerFill) {
                    spinnerFill.setAttribute('stroke-dasharray', `${status.progress}, 100`);
                }
            } else {
                btn.disabled = false;
                btn.classList.remove('is-scraping');
                if (spinnerFill) {
                    spinnerFill.setAttribute('stroke-dasharray', '0, 100');
                }
            }
        } catch (e) { }
    }

    let isRefreshing = false;

    async function handleScrape() {
        if (isRefreshing) return;
        isRefreshing = true;

        const btn = document.getElementById('btnScrape');
        const spinnerFill = document.getElementById('scrapeSpinnerFill');
        if (!btn) return;

        btn.disabled = true;
        btn.classList.add('is-scraping');

        let progress = 0;
        const totalDuration = 5000; // 5 seconds
        const intervalTime = 50;
        const increment = (intervalTime / totalDuration) * 100;

        const interval = setInterval(() => {
            progress += increment;
            if (spinnerFill) {
                spinnerFill.setAttribute('stroke-dasharray', `${Math.min(progress, 100)}, 100`);
            }

            if (progress >= 100) {
                clearInterval(interval);
                btn.disabled = false;
                btn.classList.remove('is-scraping');
                if (spinnerFill) spinnerFill.setAttribute('stroke-dasharray', '0, 100');

                // Actual refresh from DB
                loadOffers();
                const activeTab = document.querySelector('.tab-btn.active')?.dataset.tab;
                if (activeTab === 'stats') {
                    loadTechStats();
                } else if (activeTab === 'favorites') {
                    loadFavorites();
                }

                isRefreshing = false;
            }
        }, intervalTime);
    }

    // ===== DATA LOADING =====
    async function loadOffers(params = null) {
        const grid = document.getElementById('offersGrid');
        const resultsCount = document.getElementById('resultsCount');
        if (!params) params = Filters.getParams();

        grid.innerHTML = Array(4).fill('<div class="skeleton skeleton-card"></div>').join('');

        try {
            const data = await API.getOffers(params);
            renderOffers(data.offers, grid);
            renderPagination(data.page, data.total_pages, data.total);
            resultsCount.innerHTML = `<strong>${data.total}</strong> offre${data.total > 1 ? 's' : ''} trouvée${data.total > 1 ? 's' : ''}`;
        } catch (error) {
            grid.innerHTML = `
                <div class="empty-state">
                    <div class="empty-title">Erreur de chargement</div>
                    <div class="empty-text">Impossible de charger les offres.</div>
                </div>`;
            resultsCount.textContent = 'Erreur de chargement';
        }
    }

    async function loadFilters() {
        try {
            const filterData = await API.getFilters();
            Filters.populateOptions(filterData);
        } catch { }
    }

    // ===== FAVORITES =====
    let favStatusFilter = '';

    function initFavFilters() {
        document.querySelectorAll('.fav-filter-chip').forEach((chip) => {
            chip.addEventListener('click', () => {
                document.querySelectorAll('.fav-filter-chip').forEach((c) => c.classList.remove('active'));
                chip.classList.add('active');
                favStatusFilter = chip.dataset.status;
                loadFavorites();
            });
        });
    }

    async function loadFavorites() {
        const grid = document.getElementById('favGrid');
        const subtitle = document.getElementById('favSubtitle');

        if (!API.isLoggedIn()) {
            subtitle.textContent = 'Connectez-vous pour voir vos favoris';
            grid.innerHTML = `
                <div class="empty-state">
                    <div class="empty-title">Non connecté</div>
                    <div class="empty-text">Connectez-vous pour sauvegarder et suivre vos offres favorites.</div>
                </div>`;
            return;
        }

        grid.innerHTML = '<div class="skeleton skeleton-card"></div>';

        try {
            const favs = await API.getFavorites(favStatusFilter || null);
            subtitle.textContent = `${favs.length} offre${favs.length > 1 ? 's' : ''} sauvegardée${favs.length > 1 ? 's' : ''}`;

            if (favs.length === 0) {
                grid.innerHTML = `
                    <div class="empty-state">
                        <div class="empty-title">Aucun favori</div>
                        <div class="empty-text">Ajoutez des offres à vos favoris pour les retrouver ici.</div>
                    </div>`;
                return;
            }

            // Build offer responses from favorites
            const offers = favs.map((f) => ({
                ...f.offer,
                favorite_id: f.id,
                favorite_status: f.status,
            }));

            renderOffers(offers, grid, true);
        } catch (error) {
            grid.innerHTML = `
                <div class="empty-state">
                    <div class="empty-title">Erreur</div>
                    <div class="empty-text">Impossible de charger les favoris.</div>
                </div>`;
        }
    }

    async function handleStatusClick(btn) {
        if (!API.isLoggedIn()) {
            window.location.href = '/auth.html';
            return;
        }

        const offerId = btn.dataset.offerId;
        const status = btn.dataset.status;
        const favId = btn.dataset.favId || null;
        const isActive = btn.classList.contains('active');

        try {
            let newFavId = favId;
            let newStatus = status;
            let isRemoved = false;

            if (isActive && favId) {
                await API.removeFavorite(favId);
                showToast('Retiré des favoris', 'info');
                newFavId = '';
                newStatus = '';
                isRemoved = true;
            } else if (favId) {
                await API.updateFavorite(favId, { status: status });
                showToast('Statut mis à jour', 'success');
            } else {
                const newFav = await API.addFavorite(offerId, status);
                showToast('Ajouté aux favoris', 'success');
                newFavId = newFav.id;
            }

            const card = btn.closest('.offer-card');
            if (card) {
                const activeTab = document.querySelector('.tab-btn.active')?.dataset.tab;
                if (activeTab === 'favorites' && isRemoved) {
                    card.remove();
                    const grid = document.getElementById('favGrid');
                    if (grid) {
                        const count = grid.querySelectorAll('.offer-card').length;
                        if (count === 0) {
                            grid.innerHTML = `
                                <div class="empty-state">
                                    <div class="empty-title">Aucun favori</div>
                                    <div class="empty-text">Ajoutez des offres à vos favoris pour les retrouver ici.</div>
                                </div>`;
                            document.getElementById('favSubtitle').textContent = '0 offre sauvegardée';
                        } else {
                            document.getElementById('favSubtitle').textContent = `${count} offre${count > 1 ? 's' : ''} sauvegardée${count > 1 ? 's' : ''}`;
                        }
                    }
                } else {
                    const buttons = card.querySelectorAll('.btn-status');
                    buttons.forEach(b => {
                        b.dataset.favId = newFavId;
                        if (newStatus && b.dataset.status === newStatus) {
                            b.classList.add('active');
                        } else {
                            b.classList.remove('active');
                        }
                    });
                }
            }
        } catch (error) {
            showToast(error.message || 'Erreur', 'error');
        }
    }

    // ===== TECH STATISTICS =====

    async function loadTechStats(silent = false) {
        try {
            if (!cachedTechStats) cachedTechStats = await API.getTechStats();
            if (!cachedGeneralStats) cachedGeneralStats = await API.getStats();

            const stats = cachedTechStats;
            const generalStats = cachedGeneralStats;


            // Load timeline chart
            loadTimelineChart(silent);

            renderBarChart('chartCompanies', stats.top_companies, 'fw', 'keyword');
            renderBarChart('chartDepartments', stats.top_departments, 'tool', 'department');
            renderBarChart('chartCategories', stats.top_categories, 'method', 'category');
            renderBarChart('chartLanguages', stats.top_languages, 'lang', 'technology');
            renderBarChart('chartFrameworks', stats.top_frameworks, 'fw', 'technology');
            renderBarChart('chartTools', stats.top_tools, 'tool', 'technology');
            renderBarChart('chartMethodologies', stats.top_methodologies, 'method', 'technology');

            const edData = [
                { name: 'Bac+5 / Master / Ingénieur', count: generalStats.bac5_offers || 0, filterValue: 'bac+5' },
                { name: 'Bac+4 / M1', count: generalStats.bac4_offers || 0, filterValue: 'bac+4' },
                { name: 'Bac+3 / Licence / Bachelor', count: generalStats.bac3_offers || 0, filterValue: 'bac+3' },
                { name: 'Bac+2 / BTS / DUT', count: generalStats.bac2_offers || 0, filterValue: 'bac+2' },
            ].filter(d => d.count > 0);
            renderBarChart('chartEducation', edData, 'cert', 'profile');
        } catch (err) {
            console.error('Stats loading error:', err);
        }
    }

    // ===== TIMELINE CHART =====

    function initTimelineControls() {
        // Use event delegation or direct naming for reliability
        const container = document.querySelector('.timeline-controls');
        if (!container) return;

        container.addEventListener('click', (e) => {
            const btn = e.target.closest('.btn-scale');
            if (!btn) return;

            const scale = btn.dataset.scale;
            if (scale === currentTimelineScale) return;

            // UI Update
            const btns = container.querySelectorAll('.btn-scale');
            btns.forEach(b => b.classList.remove('active'));
            btn.classList.add('active');

            // State Update
            currentTimelineScale = scale;
            loadTimelineChart();
        });
    }

    async function loadTimelineChart(silent = false) {
        const loading = document.getElementById('timelineLoading');
        if (!loading) return;

        try {
            if (!silent) {
                loading.style.display = 'block';
                loading.textContent = 'Chargement des données...';
            }

            if (!cachedTimelineData[currentTimelineScale]) {
                const data = await API.getTimelineStats(currentTimelineScale);
                cachedTimelineData[currentTimelineScale] = Array.isArray(data) ? data : [];
            }

            const data = cachedTimelineData[currentTimelineScale];

            if (data && data.length > 0) {
                loading.style.display = 'none';
                renderTimelineChart(data, currentTimelineScale);
            } else {
                loading.style.display = 'block';
                loading.textContent = 'Aucune donnée d\'évolution disponible.';
            }
        } catch (err) {
            console.error('Timeline loading error:', err);
            loading.style.display = 'block';
            loading.textContent = 'Erreur de chargement.';
        }
    }

    // Singleton cleanup for chart listeners
    let timelineResizeListener = null;
    let timelineObserver = null;

    function renderTimelineChart(data, scale = 'month') {
        const container = document.getElementById('timelineChartContainer');
        const originalCanvas = document.getElementById('timelineCanvas');
        const tooltip = document.getElementById('timelineTooltip');
        if (!container || !originalCanvas || !data || data.length === 0) return;

        // Clean up old canvas to remove ALL event listeners
        const canvas = originalCanvas.cloneNode(true);
        originalCanvas.parentNode.replaceChild(canvas, originalCanvas);

        const ctx = canvas.getContext('2d');
        const dpr = window.devicePixelRatio || 1;
        const MONTH_NAMES_FR = ['Jan', 'Fév', 'Mar', 'Avr', 'Mai', 'Juin', 'Juil', 'Août', 'Sep', 'Oct', 'Nov', 'Déc'];

        function formatLabel(periodStr, isTooltip = false) {
            if (!periodStr || typeof periodStr !== 'string' || !periodStr.includes('-')) return periodStr || '';
            try {
                const parts = periodStr.split('-');
                const year = parts[0];
                const value = parseInt(parts[1], 10);
                const shortYear = year.substring(2);

                if (scale === 'week') {
                    if (isTooltip) {
                        const date = new Date(parseInt(year, 10), 0, 1 + (value - 1) * 7);
                        const mm = String(date.getMonth() + 1).padStart(2, '0');
                        const dd = String(date.getDate()).padStart(2, '0');
                        return `${dd}/${mm}/${shortYear}`;
                    }
                    return `S${value} ${year}`;
                }

                // Monthly
                return `${MONTH_NAMES_FR[value - 1]} ${year}`;
            } catch { return periodStr; }
        }

        function getThemeColors() {
            const style = getComputedStyle(document.documentElement);
            return {
                line: style.getPropertyValue('--accent-primary').trim() || '#3b82f6',
                text: style.getPropertyValue('--text-muted').trim() || '#666',
                grid: style.getPropertyValue('--border').trim() || '#1f1f1f',
                bg: style.getPropertyValue('--bg-card').trim() || '#111',
            };
        }

        let chartPoints = [];

        function draw() {
            try {
                const rect = container.getBoundingClientRect();
                canvas.width = rect.width * dpr;
                canvas.height = 280 * dpr;
                canvas.style.width = rect.width + 'px';
                canvas.style.height = '280px';
                ctx.scale(dpr, dpr);

                const w = canvas.width / dpr;
                const h = canvas.height / dpr;
                const padding = { top: 20, right: 30, bottom: 40, left: 50 };
                const chartW = w - padding.left - padding.right;
                const chartH = h - padding.top - padding.bottom;
                const colors = getThemeColors();

                ctx.clearRect(0, 0, w, h);

                const counts = data.map(d => d.count || 0);
                const maxCount = Math.max(...counts, 1);
                const range = maxCount;

                // Y Grid & labels
                ctx.textAlign = 'right';
                ctx.textBaseline = 'middle';
                ctx.font = '11px Inter, sans-serif';
                for (let i = 0; i <= 5; i++) {
                    const val = Math.round((range * i) / 5);
                    const y = padding.top + chartH - (chartH * i) / 5;
                    ctx.strokeStyle = colors.grid;
                    ctx.lineWidth = 0.5;
                    ctx.beginPath();
                    ctx.moveTo(padding.left, y);
                    ctx.lineTo(w - padding.right, y);
                    ctx.stroke();
                    ctx.fillStyle = colors.text;
                    ctx.fillText(val.toLocaleString('fr-FR'), padding.left - 8, y);
                }

                chartPoints = data.map((d, i) => ({
                    x: padding.left + (chartW * i) / (data.length - 1 || 1),
                    y: padding.top + chartH - (chartH * ((d.count || 0))) / range,
                    data: d,
                }));

                // Area Gradient
                const gradient = ctx.createLinearGradient(0, padding.top, 0, h - padding.bottom);
                gradient.addColorStop(0, colors.line + '30');
                gradient.addColorStop(1, colors.line + '00');

                ctx.beginPath();
                ctx.moveTo(chartPoints[0].x, h - padding.bottom);
                for (let i = 0; i < chartPoints.length; i++) {
                    const p = chartPoints[i];
                    if (i === 0) ctx.lineTo(p.x, p.y);
                    else {
                        const prev = chartPoints[i - 1];
                        const cpx = (prev.x + p.x) / 2;
                        ctx.bezierCurveTo(cpx, prev.y, cpx, p.y, p.x, p.y);
                    }
                }
                ctx.lineTo(chartPoints[chartPoints.length - 1].x, h - padding.bottom);
                ctx.closePath();
                ctx.fillStyle = gradient;
                ctx.fill();

                // Path
                ctx.beginPath();
                ctx.moveTo(chartPoints[0].x, chartPoints[0].y);
                for (let i = 1; i < chartPoints.length; i++) {
                    const prev = chartPoints[i - 1];
                    const p = chartPoints[i];
                    const cpx = (prev.x + p.x) / 2;
                    ctx.bezierCurveTo(cpx, prev.y, cpx, p.y, p.x, p.y);
                }
                ctx.strokeStyle = colors.line;
                ctx.lineWidth = 2.5;
                ctx.stroke();

                // X labels
                const step = Math.max(1, Math.ceil(data.length / Math.floor(chartW / 80)));
                ctx.textAlign = 'center';
                ctx.textBaseline = 'top';
                ctx.fillStyle = colors.text;
                for (let i = 0; i < data.length; i += step) {
                    ctx.fillText(formatLabel(data[i].period), chartPoints[i].x, h - padding.bottom + 10);
                }
            } catch (e) { console.error("Chart draw error:", e); }
        }

        draw();

        // Mouse Interactivity
        canvas.addEventListener('mousemove', (e) => {
            const rect = canvas.getBoundingClientRect();
            const mouseX = e.clientX - rect.left;
            let closest = null;
            let closestDist = Infinity;
            for (const p of chartPoints) {
                const d = Math.abs(mouseX - p.x);
                if (d < closestDist) { closestDist = d; closest = p; }
            }
            if (closest && closestDist < 30) {
                tooltip.style.display = 'block';
                tooltip.innerHTML = `<strong>${formatLabel(closest.data.period, true)}</strong><br>${closest.data.count} offres`;
                const tRect = tooltip.getBoundingClientRect();
                const cRect = container.getBoundingClientRect();
                let tx = closest.x + 10;
                let ty = closest.y - 10;
                if (tx + tRect.width > cRect.width) tx = closest.x - tRect.width - 10;
                tooltip.style.left = tx + 'px';
                tooltip.style.top = ty + 'px';
            } else { tooltip.style.display = 'none'; }
        });
        canvas.addEventListener('mouseleave', () => tooltip.style.display = 'none');

        // Global listeners
        if (timelineResizeListener) window.removeEventListener('resize', timelineResizeListener);
        timelineResizeListener = draw;
        window.addEventListener('resize', timelineResizeListener);
        if (timelineObserver) timelineObserver.disconnect();
        timelineObserver = new MutationObserver(draw);
        timelineObserver.observe(document.documentElement, { attributes: true, attributeFilter: ['data-theme'] });
    }

    function renderBarChart(containerId, data, cssClass, filterType) {
        const container = document.getElementById(containerId);
        if (!data || data.length === 0) {
            container.innerHTML = '<div class="stats-empty">Aucune donnée disponible.</div>';
            return;
        }
        const maxCount = Math.max(...data.map((d) => d.count));
        container.innerHTML = data.map((item) => {
            const pct = Math.max(2, (item.count / maxCount) * 100);
            const filterValue = item.filterValue || item.name;
            return `
                <div class="stats-bar-row clickable" data-filter-type="${filterType || ''}" data-filter-value="${escapeHtml(filterValue)}" title="Voir les offres : ${escapeHtml(item.name)}">
                    <span class="stats-bar-name" title="${escapeHtml(item.name)}">${escapeHtml(item.name)}</span>
                    <div class="stats-bar-track">
                        <div class="stats-bar-fill ${cssClass}" style="width: 0%" data-target="${pct}"></div>
                    </div>
                    <span class="stats-bar-count">${item.count}</span>
                </div>`;
        }).join('');

        // Add click handlers
        container.querySelectorAll('.stats-bar-row.clickable').forEach((row) => {
            row.addEventListener('click', () => {
                const fType = row.dataset.filterType;
                const fValue = row.dataset.filterValue;
                if (fType && fValue) {
                    navigateToOffersWithFilter(fType, fValue);
                }
            });
        });

        requestAnimationFrame(() => {
            setTimeout(() => {
                container.querySelectorAll('.stats-bar-fill').forEach((bar) => {
                    bar.style.width = bar.dataset.target + '%';
                });
            }, 50);
        });
    }

    /**
     * Navigate from statistics to the offers tab with a specific filter pre-filled.
     */
    function navigateToOffersWithFilter(filterKey, filterValue) {
        // Switch to offers tab
        const tabBtns = document.querySelectorAll('.tab-btn');
        tabBtns.forEach((b) => b.classList.remove('active'));
        const offersTab = document.getElementById('tabOffers');
        if (offersTab) offersTab.classList.add('active');

        document.getElementById('contentOffers').classList.remove('hidden');
        document.getElementById('contentStats').classList.add('hidden');
        document.getElementById('contentFavorites').classList.add('hidden');

        const sidebar = document.querySelector('.sidebar');
        if (sidebar) sidebar.classList.remove('hidden');

        // Update tab indicator position and color
        const tabsContainer = document.querySelector('.header-tabs');
        if (tabsContainer) tabsContainer.dataset.activeTab = 'offers';
        updateTabIndicator();

        // Set the filter and trigger reload
        Filters.setFilter(filterKey, filterValue);

        window.scrollTo({ top: 0, behavior: 'smooth' });
    }

    // ===== RENDERING =====
    function renderOffers(offers, grid, showStatus = false) {
        if (!offers || offers.length === 0) {
            grid.innerHTML = `
                <div class="empty-state">
                    <div class="empty-title">Aucune offre trouvée</div>
                    <div class="empty-text">Essayez de modifier vos filtres ou lancez un nouveau scraping.</div>
                </div>`;
            return;
        }

        grid.innerHTML = offers.map((offer) => createOfferCard(offer, showStatus)).join('');

        // Attach card click
        grid.querySelectorAll('.offer-card').forEach((card) => {
            card.addEventListener('click', (e) => {
                if (e.target.closest('.offer-link') || e.target.closest('.btn-status')) return;
                openModal(card.dataset.offerId);
            });
        });

        // Attach status button clicks
        grid.querySelectorAll('.btn-status').forEach((btn) => {
            btn.addEventListener('click', (e) => {
                e.stopPropagation();
                handleStatusClick(btn);
            });
        });
    }

    function createOfferCard(offer, showStatus = false) {
        const date = offer.publication_date ? formatDate(offer.publication_date) : 'Date inconnue';
        const description = offer.description ? truncate(offer.description, 200) : 'Pas de description disponible.';
        const sourceLabel = formatSource(offer.source);
        const skillsHtml = buildSkillsTags(offer);

        let favHtml = '';
        if (API.isLoggedIn()) {
            const fbId = offer.favorite_id || '';
            const status = offer.favorite_status || '';
            favHtml = `
            <div class="offer-fav-actions">
                <button class="btn-status to_apply ${status === 'to_apply' ? 'active' : ''}" data-offer-id="${offer.id}" data-fav-id="${fbId}" data-status="to_apply" title="A postuler">A postuler</button>
                <button class="btn-status applied ${status === 'applied' ? 'active' : ''}" data-offer-id="${offer.id}" data-fav-id="${fbId}" data-status="applied" title="Postulé">Postulé</button>
            </div>`;
        }

        return `
            <article class="offer-card" data-offer-id="${offer.id}" id="offer-${offer.id}">
                <div class="offer-header">
                    <div class="offer-title-group">
                        <h3 class="offer-title">${escapeHtml(offer.title)}</h3>
                        <span class="offer-source">${sourceLabel}</span>
                    </div>
                    ${favHtml}
                </div>
                <div class="offer-company">${escapeHtml(offer.company)}</div>
                <div class="offer-meta">
                    ${offer.location ? `<span class="offer-meta-item">${escapeHtml(offer.location)}</span>` : ''}
                    ${offer.contract_type ? `<span class="offer-meta-item">${escapeHtml(offer.contract_type)}</span>` : ''}
                    ${offer.salary ? `<span class="offer-meta-item">${escapeHtml(offer.salary)}</span>` : ''}
                    ${offer.profile ? `<span class="offer-meta-item">${escapeHtml(offer.profile)}</span>` : ''}
                </div>
                <p class="offer-description">${escapeHtml(description)}</p>
                ${skillsHtml}
                <div class="offer-footer">
                    <span class="offer-date">${date}</span>
                    ${offer.url ? `<a class="offer-link" href="${escapeHtml(offer.url)}" target="_blank" rel="noopener">Voir l'offre &rarr;</a>` : ''}
                </div>
            </article>`;
    }

    function buildSkillsTags(offer) {
        const tags = [];
        if (offer.skills_languages?.length) offer.skills_languages.forEach((s) => tags.push(`<span class="skill-tag lang">${escapeHtml(s)}</span>`));
        if (offer.skills_frameworks?.length) offer.skills_frameworks.forEach((s) => tags.push(`<span class="skill-tag fw">${escapeHtml(s)}</span>`));
        if (offer.skills_tools?.length) offer.skills_tools.slice(0, 5).forEach((s) => tags.push(`<span class="skill-tag tool">${escapeHtml(s)}</span>`));
        if (offer.skills_methodologies?.length) offer.skills_methodologies.slice(0, 3).forEach((s) => tags.push(`<span class="skill-tag method">${escapeHtml(s)}</span>`));
        if (offer.skills_certifications?.length) offer.skills_certifications.forEach((s) => tags.push(`<span class="skill-tag cert">${escapeHtml(s)}</span>`));
        return tags.length ? `<div class="offer-skills">${tags.join('')}</div>` : '';
    }

    function renderPagination(currentPage, totalPages, totalItems) {
        const container = document.getElementById('pagination');
        if (totalPages <= 1) { container.innerHTML = ''; return; }

        let html = `<button ${currentPage <= 1 ? 'disabled' : ''} data-page="${currentPage - 1}">&lsaquo;</button>`;
        const maxVisible = 7;
        let startPage = Math.max(1, currentPage - Math.floor(maxVisible / 2));
        let endPage = Math.min(totalPages, startPage + maxVisible - 1);
        if (endPage - startPage < maxVisible - 1) startPage = Math.max(1, endPage - maxVisible + 1);

        if (startPage > 1) {
            html += `<button data-page="1">1</button>`;
            if (startPage > 2) html += `<span class="pagination-info">&hellip;</span>`;
        }
        for (let i = startPage; i <= endPage; i++) {
            html += `<button class="${i === currentPage ? 'active' : ''}" data-page="${i}">${i}</button>`;
        }
        if (endPage < totalPages) {
            if (endPage < totalPages - 1) html += `<span class="pagination-info">&hellip;</span>`;
            html += `<button data-page="${totalPages}">${totalPages}</button>`;
        }
        html += `<button ${currentPage >= totalPages ? 'disabled' : ''} data-page="${currentPage + 1}">&rsaquo;</button>`;

        container.innerHTML = html;
        container.querySelectorAll('button[data-page]').forEach((btn) => {
            btn.addEventListener('click', () => {
                const page = parseInt(btn.dataset.page);
                if (page >= 1 && page <= totalPages) {
                    Filters.setPage(page);
                    window.scrollTo({ top: 0, behavior: 'smooth' });
                }
            });
        });
    }

    // ===== MODAL =====
    async function openModal(offerId) {
        const overlay = document.getElementById('modalOverlay');
        try {
            const offer = await API.getOffer(offerId);
            currentModalOffer = offer;

            document.getElementById('modalTitle').textContent = offer.title;
            document.getElementById('modalCompany').textContent = offer.company;

            const infoGrid = document.getElementById('modalInfoGrid');
            const infoItems = [
                { label: 'Localisation', value: offer.location },
                { label: 'Type de contrat', value: offer.contract_type },
                { label: 'Salaire', value: offer.salary },
                { label: 'Profil demandé', value: offer.profile },
                { label: 'Source', value: formatSource(offer.source) },
                { label: 'Date de publication', value: offer.publication_date ? formatDate(offer.publication_date) : null },
            ].filter((item) => item.value);

            infoGrid.innerHTML = infoItems.map((item) => `
                <div class="modal-info-item">
                    <div class="modal-info-label">${item.label}</div>
                    <div class="modal-info-value">${escapeHtml(item.value)}</div>
                </div>`).join('');

            renderModalSkills(offer);

            document.getElementById('modalDescription').textContent = offer.description || 'Description non disponible.';

            const modalLink = document.getElementById('modalLink');
            if (offer.url) {
                modalLink.href = offer.url;
                modalLink.style.display = 'inline-flex';
            } else {
                modalLink.style.display = 'none';
            }

            overlay.classList.add('active');
            document.body.style.overflow = 'hidden';
        } catch {
            showToast("Impossible de charger les détails.", 'error');
        }
    }

    function renderModalSkills(offer) {
        const container = document.getElementById('modalSkills');
        const grid = document.getElementById('modalSkillsGrid');
        const groups = [
            { label: 'Langages', items: offer.skills_languages, css: 'lang' },
            { label: 'Frameworks', items: offer.skills_frameworks, css: 'fw' },
            { label: 'Outils', items: offer.skills_tools, css: 'tool' },
            { label: 'Certifications', items: offer.skills_certifications, css: 'cert' },
            { label: 'Méthodes', items: offer.skills_methodologies, css: 'method' },
        ];
        const hasSkills = groups.some((g) => g.items?.length > 0);
        if (!hasSkills) { container.style.display = 'none'; return; }

        container.style.display = 'block';
        grid.innerHTML = groups.filter((g) => g.items?.length > 0).map((g) => `
            <div class="modal-skill-group">
                <span class="modal-skill-label">${g.label}</span>
                <div class="modal-skill-tags">
                    ${g.items.map((s) => `<span class="skill-tag ${g.css}">${escapeHtml(s)}</span>`).join('')}
                </div>
            </div>`).join('');
    }

    function closeModal() {
        document.getElementById('modalOverlay').classList.remove('active');
        document.body.style.overflow = '';
        currentModalOffer = null;
    }

    // ===== TOASTS =====
    function showToast(message, type = 'info') {
        return; // Notifications disabled
    }

    // ===== UTILITIES =====
    function formatDate(dateStr) {
        try {
            const date = new Date(dateStr);
            const now = new Date();
            const diff = now - date;
            const days = Math.floor(diff / (1000 * 60 * 60 * 24));
            if (days === 0) return "Aujourd'hui";
            if (days === 1) return 'Hier';
            if (days < 7) return `Il y a ${days} jours`;
            if (days < 30) return `Il y a ${Math.floor(days / 7)} sem.`;
            return date.toLocaleDateString('fr-FR', { day: 'numeric', month: 'short', year: 'numeric' });
        } catch { return dateStr; }
    }

    function formatNumber(num) {
        if (num === undefined || num === null) return '–';
        return num.toLocaleString('fr-FR');
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

    function truncate(str, maxLen) {
        if (!str || str.length <= maxLen) return str;
        return str.substring(0, maxLen) + '…';
    }

    function escapeHtml(text) {
        if (!text) return '';
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }
});
