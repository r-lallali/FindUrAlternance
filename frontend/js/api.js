/**
 * API client for the FUA Alternance Dashboard.
 * Handles all HTTP calls to the FastAPI backend, including auth.
 */

const API = (() => {
    const BASE_URL = '/api';

    function getToken() {
        return localStorage.getItem('fua-token');
    }

    function authHeaders() {
        const token = getToken();
        const headers = { 'Content-Type': 'application/json' };
        if (token) headers['Authorization'] = `Bearer ${token}`;
        return headers;
    }

    async function request(endpoint, options = {}) {
        const url = `${BASE_URL}${endpoint}`;
        try {
            const response = await fetch(url, {
                headers: authHeaders(),
                ...options,
            });

            if (response.status === 401) {
                localStorage.removeItem('fua-token');
                localStorage.removeItem('fua-user');
            }

            if (response.status === 204) return null;

            if (!response.ok) {
                const errorData = await response.json().catch(() => ({}));
                throw new Error(errorData.detail || `HTTP ${response.status}`);
            }

            return await response.json();
        } catch (error) {
            console.error(`API Error [${endpoint}]:`, error);
            throw error;
        }
    }

    function buildQuery(params) {
        const query = new URLSearchParams();
        for (const [key, value] of Object.entries(params)) {
            if (value !== null && value !== undefined && value !== '') {
                query.append(key, value);
            }
        }
        return query.toString();
    }

    return {
        // ── Offers ──
        getOffers(params = {}) {
            const query = buildQuery(params);
            return request(`/offers${query ? '?' + query : ''}`);
        },
        getOffer(id) {
            return request(`/offers/${id}`);
        },
        fetchOfferDescription(id) {
            return request(`/offers/${id}/fetch-description`, { method: 'POST' });
        },
        getFilters() {
            return request('/filters');
        },
        getStats() {
            return request('/stats');
        },
        getTechStats() {
            return request('/stats/tech');
        },
        getTimelineStats(scale = 'month') {
            return request(`/stats/timeline?scale=${scale}`);
        },

        // ── Scraping ──
        scrapeAll() {
            return request('/scrape', { method: 'POST' });
        },
        scrapeSource(source) {
            return request(`/scrape/${source}`, { method: 'POST' });
        },
        getScrapeStatus() {
            return request('/scrape/status');
        },

        // ── Auth ──
        register(username, email, password) {
            return request('/auth/register', {
                method: 'POST',
                body: JSON.stringify({ username, email, password }),
            });
        },
        login(email, password) {
            return request('/auth/login', {
                method: 'POST',
                body: JSON.stringify({ email, password }),
            });
        },
        getMe() {
            return request('/auth/me');
        },

        // ── Favorites ──
        getFavorites(status = null) {
            const query = status ? `?status=${status}` : '';
            return request(`/favorites${query}`);
        },
        addFavorite(offerId, status = 'to_apply') {
            return request('/favorites', {
                method: 'POST',
                body: JSON.stringify({ offer_id: offerId, status }),
            });
        },
        updateFavorite(favoriteId, data) {
            return request(`/favorites/${favoriteId}`, {
                method: 'PUT',
                body: JSON.stringify(data),
            });
        },
        removeFavorite(favoriteId) {
            return request(`/favorites/${favoriteId}`, { method: 'DELETE' });
        },

        // ── Helpers ──
        isLoggedIn() {
            return !!getToken();
        },
        getUser() {
            try {
                return JSON.parse(localStorage.getItem('fua-user'));
            } catch {
                return null;
            }
        },
        logout() {
            localStorage.removeItem('fua-token');
            localStorage.removeItem('fua-user');
        },
    };
})();
