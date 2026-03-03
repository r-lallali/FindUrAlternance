/**
 * Main application for the FUA Alternance Dashboard.
 * Orchestrates API calls, rendering, user interactions, favorites, and statistics.
 */

document.addEventListener('DOMContentLoaded', () => {
    // State
    let currentModalOffer = null;

    // ===== INITIALIZATION =====
    initTheme();
    initUserUI();
    Filters.init(handleFilterChange);
    loadStats();
    loadFilters();
    loadOffers();
    initTabs();
    initModal();
    initFavFilters();

    const btnScrape = document.getElementById('btnScrape');
    if (btnScrape) btnScrape.addEventListener('click', handleScrape);

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
            });
        });
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

    async function handleScrape() {
        const btn = document.getElementById('btnScrape');
        btn.disabled = true;
        btn.classList.add('loading');
        btn.textContent = 'Démarrage...';

        try {
            await API.scrapeAll();
            showToast('Le scraping a démarré en tâche de fond. Actualisez dans quelques minutes.', 'success');
        } catch (error) {
            showToast('Erreur lors du lancement du scraping.', 'error');
        } finally {
            btn.disabled = false;
            btn.classList.remove('loading');
            btn.textContent = 'Actualiser';
        }
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

    async function loadStats() {
        try {
            const stats = await API.getStats();
            document.getElementById('statTotal').textContent = formatNumber(stats.total_offers);
            document.getElementById('statIT').textContent = formatNumber(stats.it_offers || 0);
            document.getElementById('statRecent').textContent = formatNumber(stats.recent_24h);
            document.getElementById('statSources').textContent = Object.keys(stats.by_source).length;
        } catch { }
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
    let cachedTechStats = null;
    let cachedGeneralStats = null;

    async function loadTechStats() {
        try {
            if (!cachedTechStats) cachedTechStats = await API.getTechStats();
            if (!cachedGeneralStats) cachedGeneralStats = await API.getStats();

            const stats = cachedTechStats;
            const generalStats = cachedGeneralStats;

            document.getElementById('statsSubtitle').textContent =
                `${formatNumber(stats.total_it_offers)} offres IT analysées sur ${formatNumber(stats.total_offers)} offres totales`;

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
        } catch {
            document.getElementById('statsSubtitle').textContent = 'Erreur lors du chargement.';
        }
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
                <button class="btn-status rejected ${status === 'rejected' ? 'active' : ''}" data-offer-id="${offer.id}" data-fav-id="${fbId}" data-status="rejected" title="Refusé">Refusé</button>
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
        const container = document.getElementById('toastContainer');
        const toast = document.createElement('div');
        toast.className = `toast ${type}`;
        toast.textContent = message;
        container.appendChild(toast);
        setTimeout(() => {
            toast.style.opacity = '0';
            toast.style.transform = 'translateX(100%)';
            toast.style.transition = 'all 0.3s ease';
            setTimeout(() => toast.remove(), 300);
        }, 4000);
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
