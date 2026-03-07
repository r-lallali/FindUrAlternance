"""API routes for the alternance dashboard."""

import json
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Optional, Any
from fastapi import APIRouter, Depends, Query, HTTPException, status, BackgroundTasks
from sqlalchemy.orm import Session
from sqlalchemy import func, or_, desc, cast
from sqlalchemy.dialects.postgresql import JSONB

from database import get_db
from models import Offer, User, Favorite, ScrapingLog
from schemas import (
    OfferListResponse, OfferResponse, FilterOptions, ScrapingStatus, TechStats,
    UserRegister, UserLogin, UserResponse, TokenResponse,
    FavoriteCreate, FavoriteUpdate, FavoriteResponse,
)
from auth import hash_password, verify_password, create_token, get_current_user, get_optional_user

router = APIRouter(prefix="/api", tags=["offers"])

# ─── IN-MEMORY CACHE FOR STATS ───
class StatsCache:
    def __init__(self):
        self._cache = {}
        self._ttl = 600  # 10 minutes

    def get(self, key: str) -> Optional[Any]:
        if key in self._cache:
            entry = self._cache[key]
            if time.time() - entry['timestamp'] < self._ttl:
                return entry['data']
            else:
                del self._cache[key]
        return None

    def set(self, key: str, data: Any):
        self._cache[key] = {
            'data': data,
            'timestamp': time.time()
        }

    def clear(self):
        self._cache = {}

global_stats_cache = StatsCache()


# ═══════════════════════════════════════════════════════
# AUTH ENDPOINTS
# ═══════════════════════════════════════════════════════

@router.post("/auth/register", response_model=TokenResponse)
async def register(data: UserRegister, db: Session = Depends(get_db)):
    """Register a new user."""
    # Validate input
    if len(data.username) < 2:
        raise HTTPException(status_code=400, detail="Le pseudo doit contenir au moins 2 caractères")
    if len(data.password) < 6:
        raise HTTPException(status_code=400, detail="Le mot de passe doit contenir au moins 6 caractères")
    if "@" not in data.email:
        raise HTTPException(status_code=400, detail="Email invalide")

    # Check duplicates
    if db.query(User).filter(User.email == data.email.lower()).first():
        raise HTTPException(status_code=409, detail="Cet email est déjà utilisé")
    if db.query(User).filter(User.username == data.username).first():
        raise HTTPException(status_code=409, detail="Ce pseudo est déjà pris")

    user = User(
        username=data.username,
        email=data.email.lower(),
        password_hash=hash_password(data.password),
    )
    db.add(user)
    db.commit()
    db.refresh(user)

    token = create_token(user.id, user.username)
    return TokenResponse(
        access_token=token,
        user=UserResponse.model_validate(user),
    )


@router.post("/auth/login", response_model=TokenResponse)
async def login(data: UserLogin, db: Session = Depends(get_db)):
    """Login and return a JWT token."""
    user = db.query(User).filter(User.email == data.email.lower()).first()
    if not user or not verify_password(data.password, user.password_hash):
        raise HTTPException(status_code=401, detail="Email ou mot de passe incorrect")

    token = create_token(user.id, user.username)
    return TokenResponse(
        access_token=token,
        user=UserResponse.model_validate(user),
    )


@router.get("/auth/me", response_model=UserResponse)
async def get_me(user: User = Depends(get_current_user)):
    """Get current user profile."""
    return UserResponse.model_validate(user)


# ═══════════════════════════════════════════════════════
# FAVORITES ENDPOINTS
# ═══════════════════════════════════════════════════════

@router.get("/favorites", response_model=list[FavoriteResponse])
async def get_favorites(
    status_filter: Optional[str] = Query(None, alias="status", description="Filter by status"),
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Get all favorites for the current user."""
    query = db.query(Favorite).filter(Favorite.user_id == user.id)
    if status_filter:
        query = query.filter(Favorite.status == status_filter)
    query = query.order_by(Favorite.updated_at.desc())
    favorites = query.all()

    result = []
    for fav in favorites:
        fav_dict = FavoriteResponse.model_validate(fav)
        fav_dict.offer = OfferResponse.model_validate(fav.offer) if fav.offer else None
        result.append(fav_dict)

    return result


@router.post("/favorites", response_model=FavoriteResponse, status_code=201)
async def add_favorite(
    data: FavoriteCreate,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Add an offer to favorites."""
    # Verify offer exists
    offer = db.query(Offer).filter(Offer.id == data.offer_id).first()
    if not offer:
        raise HTTPException(status_code=404, detail="Offre introuvable")

    # Check if already favorited
    existing = db.query(Favorite).filter(
        Favorite.user_id == user.id,
        Favorite.offer_id == data.offer_id,
    ).first()
    if existing:
        raise HTTPException(status_code=409, detail="Offre déjà dans les favoris")

    # Validate status
    valid_statuses = {"to_apply", "applied", "rejected"}
    if data.status not in valid_statuses:
        raise HTTPException(status_code=400, detail=f"Statut invalide. Valeurs acceptées : {valid_statuses}")

    fav = Favorite(
        user_id=user.id,
        offer_id=data.offer_id,
        status=data.status,
        notes=data.notes,
    )
    db.add(fav)
    db.commit()
    db.refresh(fav)

    resp = FavoriteResponse.model_validate(fav)
    resp.offer = OfferResponse.model_validate(fav.offer)
    return resp


@router.put("/favorites/{favorite_id}", response_model=FavoriteResponse)
async def update_favorite(
    favorite_id: str,
    data: FavoriteUpdate,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Update a favorite's status or notes."""
    fav = db.query(Favorite).filter(
        Favorite.id == favorite_id,
        Favorite.user_id == user.id,
    ).first()
    if not fav:
        raise HTTPException(status_code=404, detail="Favori introuvable")

    valid_statuses = {"to_apply", "applied", "rejected"}
    if data.status is not None:
        if data.status not in valid_statuses:
            raise HTTPException(status_code=400, detail=f"Statut invalide. Valeurs acceptées : {valid_statuses}")
        fav.status = data.status

    if data.notes is not None:
        fav.notes = data.notes

    fav.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(fav)

    resp = FavoriteResponse.model_validate(fav)
    resp.offer = OfferResponse.model_validate(fav.offer)
    return resp


@router.delete("/favorites/{favorite_id}", status_code=204)
async def remove_favorite(
    favorite_id: str,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Remove a favorite."""
    fav = db.query(Favorite).filter(
        Favorite.id == favorite_id,
        Favorite.user_id == user.id,
    ).first()
    if not fav:
        raise HTTPException(status_code=404, detail="Favori introuvable")

    db.delete(fav)
    db.commit()


# ═══════════════════════════════════════════════════════
# OFFERS ENDPOINTS
# ═══════════════════════════════════════════════════════

def _base_query(db: Session):
    """Base query: exclude school offers, non-alternance, and older than 90 days."""
    three_months_ago = (datetime.utcnow() - timedelta(days=90)).replace(hour=0, minute=0, second=0, microsecond=0)
    return db.query(Offer).filter(
        Offer.is_school == False,  # noqa: E712
        Offer.is_alternance == True,   # noqa: E712
        Offer.publication_date >= three_months_ago
    )


@router.get("/offers", response_model=OfferListResponse)
async def get_offers(
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
    keyword: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    company: Optional[str] = Query(None),
    location: Optional[str] = Query(None),
    department: Optional[str] = Query(None),
    contract_type: Optional[str] = Query(None),
    profile: Optional[str] = Query(None),
    source: Optional[str] = Query(None),
    technology: Optional[str] = Query(None),
    salary_min: Optional[str] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    date_filter: Optional[str] = Query(None),
    sort_by: Optional[str] = Query("date"),
    sort_order: Optional[str] = Query("desc"),
    user: Optional[User] = Depends(get_optional_user),
    db: Session = Depends(get_db),
):
    """Get paginated and filtered offers."""
    query = _base_query(db)

    # Exclude offers favorited by the current user
    if user:
        query = query.filter(
            ~Offer.id.in_(
                db.query(Favorite.offer_id).filter(Favorite.user_id == user.id)
            )
        )

    if keyword:
        keyword_filter = f"%{keyword}%"
        query = query.filter(
            or_(
                Offer.title.ilike(keyword_filter),
                Offer.description.ilike(keyword_filter),
                Offer.company.ilike(keyword_filter),
            )
        )
    if category:
        query = query.filter(Offer.category.ilike(category))
    if company:
        query = query.filter(func.trim(Offer.company).ilike(company))
    if location:
        from scrapers.utils import extract_department
        dept_match = extract_department(location)
        if dept_match:
            # If the search is exactly the department code, use it strictly
            if location.strip() == dept_match:
                query = query.filter(Offer.department == dept_match)
            else:
                # If it's something like "Paris 01", we want either exact text match or the resolved department
                query = query.filter(
                    or_(
                        Offer.location.ilike(f"%{location}%"),
                        Offer.department == dept_match
                    )
                )
        else:
            query = query.filter(Offer.location.ilike(f"%{location}%"))
    if department:
        query = query.filter(Offer.department == department)
    if contract_type:
        query = query.filter(Offer.contract_type.ilike(f"%{contract_type}%"))
    if profile:
        profile_lower = str(profile).lower()
        if profile_lower == "bac+3":
            query = query.filter(
                or_(
                    Offer.profile.ilike("%bac+3%"),
                    Offer.profile.ilike("%licence%"),
                    Offer.profile.ilike("%bachelor%"),
                    Offer.description.ilike("%bac + 3%"),
                    Offer.description.ilike("%licence%"),
                    Offer.description.ilike("%bachelor%"),
                    Offer.title.ilike("%bac+3%"),
                    Offer.title.ilike("%bac + 3%"),
                    Offer.title.ilike("%licence%"),
                    Offer.title.ilike("%bachelor%")
                )
            )
        elif profile_lower == "bac+4":
            query = query.filter(
                or_(
                    Offer.profile.ilike("%bac+4%"),
                    Offer.profile.ilike("%m1%"),
                    Offer.profile.ilike("%maîtrise%"),
                    Offer.description.ilike("%bac + 4%"),
                    Offer.description.ilike("%m1%"),
                    Offer.description.ilike("%maîtrise%"),
                    Offer.title.ilike("%bac+4%"),
                    Offer.title.ilike("%bac + 4%"),
                    Offer.title.ilike("%m1%")
                )
            )
        elif profile_lower == "bac+5":
            query = query.filter(
                or_(
                    Offer.profile.ilike("%bac+5%"),
                    Offer.profile.ilike("%master%"),
                    Offer.profile.ilike("%m2%"),
                    Offer.profile.ilike("%ingénieur%"),
                    Offer.description.ilike("%bac + 5%"),
                    Offer.description.ilike("%master%"),
                    Offer.description.ilike("%m2%"),
                    Offer.description.ilike("%ingénieur%"),
                    Offer.title.ilike("%bac+5%"),
                    Offer.title.ilike("%bac + 5%"),
                    Offer.title.ilike("%master%"),
                    Offer.title.ilike("%m2%"),
                    Offer.title.ilike("%ingénieur%")
                )
            )
        elif profile_lower == "bac+2":
            query = query.filter(
                or_(
                    Offer.profile.ilike("%bac+2%"),
                    Offer.profile.ilike("%bts%"),
                    Offer.profile.ilike("%dut%"),
                    Offer.description.ilike("%bac + 2%"),
                    Offer.description.ilike("%bts%"),
                    Offer.description.ilike("%dut%"),
                    Offer.title.ilike("%bac+2%"),
                    Offer.title.ilike("%bac + 2%"),
                    Offer.title.ilike("%bts%"),
                    Offer.title.ilike("%dut%")
                )
            )
        else:
            query = query.filter(Offer.profile == profile)
    if source:
        query = query.filter(Offer.source.ilike(source))
    if technology:
        # Improved tech search: try to match as a full string in the JSON or as a word in the text skills
        tech_pattern = f'%"{technology}"%'
        query = query.filter(
            or_(
                Offer.skills_all.ilike(tech_pattern),
                Offer.skills_languages.ilike(tech_pattern),
                Offer.skills_frameworks.ilike(tech_pattern),
                Offer.skills_tools.ilike(tech_pattern),
                Offer.skills_certifications.ilike(tech_pattern),
                Offer.skills_methodologies.ilike(tech_pattern)
            )
        )

    # Date filters
    if date_filter:
        now = datetime.utcnow()
        if date_filter == "today":
            query = query.filter(Offer.publication_date >= now.replace(hour=0, minute=0, second=0, microsecond=0))
        elif date_filter == "week":
            query = query.filter(Offer.publication_date >= (now - timedelta(days=7)).replace(hour=0, minute=0, second=0, microsecond=0))
        elif date_filter == "month":
            query = query.filter(Offer.publication_date >= (now - timedelta(days=30)).replace(hour=0, minute=0, second=0, microsecond=0))
    if date_from:
        try:
            query = query.filter(Offer.publication_date >= datetime.strptime(date_from, "%Y-%m-%d"))
        except ValueError:
            pass
    if date_to:
        try:
            query = query.filter(Offer.publication_date <= datetime.strptime(date_to, "%Y-%m-%d"))
        except ValueError:
            pass

    # Sorting
    sort_column = {"date": Offer.publication_date, "title": Offer.title, "company": Offer.company}.get(sort_by, Offer.publication_date)
    if sort_order == "asc":
        query = query.order_by(sort_column.asc().nullslast())
    else:
        query = query.order_by(sort_column.desc().nullsfirst())

    total = query.count()
    offset = (page - 1) * per_page
    offers = query.offset(offset).limit(per_page).all()
    total_pages = max(1, (total + per_page - 1) // per_page)

    # Build response with favorite info if user is logged in
    user_favorites = {}
    if user:
        favs = db.query(Favorite).filter(Favorite.user_id == user.id).all()
        user_favorites = {f.offer_id: (f.id, f.status) for f in favs}

    offer_responses = []
    for o in offers:
        resp = OfferResponse.model_validate(o)
        if o.id in user_favorites:
            resp.favorite_id, resp.favorite_status = user_favorites[o.id]
        offer_responses.append(resp)

    return OfferListResponse(
        offers=offer_responses,
        total=total,
        page=page,
        per_page=per_page,
        total_pages=total_pages,
    )


@router.get("/offers/{offer_id}", response_model=OfferResponse)
async def get_offer(
    offer_id: str,
    user: Optional[User] = Depends(get_optional_user),
    db: Session = Depends(get_db),
):
    """Get a single offer by ID."""
    offer = db.query(Offer).filter(Offer.id == offer_id).first()
    if not offer:
        raise HTTPException(status_code=404, detail="Offer not found")

    resp = OfferResponse.model_validate(offer)
    if user:
        fav = db.query(Favorite).filter(
            Favorite.user_id == user.id,
            Favorite.offer_id == offer_id,
        ).first()
        if fav:
            resp.favorite_id = fav.id
            resp.favorite_status = fav.status

    return resp


@router.get("/filters", response_model=FilterOptions)
async def get_filter_options(
    user: Optional[User] = Depends(get_optional_user),
    db: Session = Depends(get_db)
):
    """Get available filter options based on current data."""
    base_query = _base_query(db)
    
    # Exclude offers favorited by the current user to match the offers list
    if user:
        base_query = base_query.filter(
            ~Offer.id.in_(
                db.query(Favorite.offer_id).filter(Favorite.user_id == user.id)
            )
        )

    categories = [
        r[0] for r in base_query.with_entities(Offer.category)
        .filter(Offer.category.isnot(None), Offer.category != "")
        .distinct().order_by(Offer.category).limit(50).all()
    ]
    locations = [
        r[0] for r in base_query.with_entities(Offer.location)
        .filter(Offer.location.isnot(None), Offer.location != "")
        .distinct().order_by(Offer.location).limit(100).all()
    ]
    departments = [
        r[0] for r in base_query.with_entities(Offer.department)
        .filter(Offer.department.isnot(None), Offer.department != "")
        .distinct().order_by(Offer.department).all()
    ]
    contract_types = [
        r[0] for r in base_query.with_entities(Offer.contract_type)
        .filter(Offer.contract_type.isnot(None), Offer.contract_type != "")
        .distinct().order_by(Offer.contract_type).all()
    ]
    profiles = [
        r[0] for r in base_query.with_entities(Offer.profile)
        .filter(Offer.profile.isnot(None), Offer.profile != "")
        .distinct().order_by(Offer.profile).all()
    ]
    sources = [
        r[0] for r in base_query.with_entities(Offer.source)
        .distinct().order_by(Offer.source).all()
    ]
    technologies = _aggregate_technologies(base_query)

    return FilterOptions(
        categories=categories,
        locations=locations,
        departments=departments,
        contract_types=contract_types,
        profiles=profiles,
        sources=sources,
        technologies=technologies,
    )


def _aggregate_technologies(query) -> list[str]:
    """Aggregate technologies using a single fetch + Python Counter for speed."""
    counter = Counter()
    results = query.with_entities(Offer.skills_all).filter(
        Offer.skills_all.isnot(None), Offer.skills_all != "[]"
    ).all()
    for (skills_json,) in results:
        try:
            skills = json.loads(skills_json)
            if isinstance(skills, list):
                counter.update(skills)
        except Exception:
            pass
    return [tech for tech, _ in counter.most_common(50)]


@router.get("/stats")
async def get_stats(
    user: Optional[User] = Depends(get_optional_user),
    db: Session = Depends(get_db)
):
    """Get dashboard statistics."""
    cache_key = f"stats_{user.id if user else 'anon'}"
    cached = global_stats_cache.get(cache_key)
    if cached:
        return cached

    base_query = _base_query(db)
    
    # Exclude offers favorited by the current user to match the search results
    if user:
        base_query = base_query.filter(
            ~Offer.id.in_(
                db.query(Favorite.offer_id).filter(Favorite.user_id == user.id)
            )
        )

    # Single query for all counts to reduce roundtrips
    now = datetime.utcnow()
    counts = base_query.with_entities(
        func.count(Offer.id),
        func.count(Offer.id).filter(Offer.publication_date >= now - timedelta(hours=24)),
        func.count(Offer.id).filter(Offer.skills_all.isnot(None), Offer.skills_all != "[]"),
        # Education counts using filter
        func.count(Offer.id).filter(or_(
            Offer.profile.ilike("%bac+2%"), Offer.description.ilike("%bac+2%"), Offer.description.ilike("%bac + 2%"),
            Offer.description.ilike("%bts%"), Offer.description.ilike("%dut%"),
            Offer.title.ilike("%bac+2%"), Offer.title.ilike("%bac + 2%"), Offer.title.ilike("%bts%"), Offer.title.ilike("%dut%")
        )),
        func.count(Offer.id).filter(or_(
            Offer.profile.ilike("%bac+3%"), Offer.description.ilike("%bac+3%"), Offer.description.ilike("%bac + 3%"),
            Offer.description.ilike("%licence%"), Offer.description.ilike("%bachelor%"),
            Offer.title.ilike("%bac+3%"), Offer.title.ilike("%bac + 3%"), Offer.title.ilike("%licence%"), Offer.title.ilike("%bachelor%")
        )),
        func.count(Offer.id).filter(or_(
            Offer.profile.ilike("%bac+4%"), Offer.description.ilike("%bac+4%"), Offer.description.ilike("%bac + 4%"),
            Offer.description.ilike("%m1%"), Offer.description.ilike("%maîtrise%"),
            Offer.title.ilike("%bac+4%"), Offer.title.ilike("%bac + 4%"), Offer.title.ilike("%m1%")
        )),
        func.count(Offer.id).filter(or_(
            Offer.profile.ilike("%bac+5%"), Offer.profile.ilike("%master%"), Offer.profile.ilike("%m2%"), Offer.profile.ilike("%ingénieur%"),
            Offer.description.ilike("%bac+5%"), Offer.description.ilike("%bac + 5%"), Offer.description.ilike("%master%"),
            Offer.description.ilike("%m2%"), Offer.description.ilike("%ingénieur%"),
            Offer.title.ilike("%bac+5%"), Offer.title.ilike("%bac + 5%"), Offer.title.ilike("%master%"), Offer.title.ilike("%m2%"), Offer.title.ilike("%ingénieur%")
        ))
    ).first()

    (total_offers, recent, it_offers, bac2_offers, bac3_offers, bac4_offers, bac5_offers) = counts or (0,0,0,0,0,0,0)

    by_source = dict(
        base_query.with_entities(Offer.source, func.count(Offer.id))
        .group_by(Offer.source).all()
    )
    by_category = dict(
        base_query.with_entities(func.trim(Offer.category), func.count(Offer.id))
        .filter(Offer.category.isnot(None), Offer.category != "")
        .group_by(func.trim(Offer.category))
        .order_by(desc(func.count(Offer.id)))
        .limit(10).all()
    )

    res = {
        "total_offers": total_offers,
        "by_source": by_source,
        "by_category": by_category,
        "recent_24h": recent,
        "it_offers": it_offers,
        "bac2_offers": bac2_offers,
        "bac3_offers": bac3_offers,
        "bac4_offers": bac4_offers,
        "bac5_offers": bac5_offers,
    }
    global_stats_cache.set(cache_key, res)
    return res


@router.get("/stats/tech", response_model=TechStats)
async def get_tech_stats(
    user: Optional[User] = Depends(get_optional_user),
    db: Session = Depends(get_db)
):
    """Get detailed technology statistics with maximum performance (One-pass scan)."""
    cache_key = f"tech_stats_{user.id if user else 'anon'}"
    cached = global_stats_cache.get(cache_key)
    if cached:
        return cached

    base_query = _base_query(db)

    if user:
        base_query = base_query.filter(
            ~Offer.id.in_(
                db.query(Favorite.offer_id).filter(Favorite.user_id == user.id)
            )
        )

    # ONE single fetch for all relevant data needed for stats
    # 3300 rows * 8 fields = ~26k data points, perfectly fits in memory for rapid sorting
    all_data = base_query.with_entities(
        Offer.skills_languages,
        Offer.skills_frameworks,
        Offer.skills_tools,
        Offer.skills_certifications,
        Offer.skills_methodologies,
        Offer.company,
        Offer.title,
        Offer.description,
        Offer.department,
        Offer.category
    ).all()

    total_offers = len(all_data)
    
    # Simple counters for skill sets
    lang_counter = Counter()
    fw_counter = Counter()
    tool_counter = Counter()
    cert_counter = Counter()
    method_counter = Counter()
    
    # Counters for metadata
    company_field_counter = Counter()  # Base counts for identification
    dept_counter = Counter()
    cat_counter = Counter()
    
    # List to store normalized match data for the accurate company counts
    match_list = []
    it_offers_count = 0

    for langs, fws, tools, certs, methods, company, title, desc_text, dept, cat in all_data:
        # 1. Process Metadata
        if company: company_field_counter.update([company.strip()])
        if dept: dept_counter.update([dept.strip()])
        if cat: cat_counter.update([cat.strip()])
        
        # 2. Process Skills
        has_skills = False
        for data, counter in [(langs, lang_counter), (fws, fw_counter), (tools, tool_counter), 
                             (certs, cert_counter), (methods, method_counter)]:
            if data:
                try:
                    items = json.loads(data)
                    if isinstance(items, list):
                        counter.update(items)
                        if items: has_skills = True
                except Exception: pass
        
        if has_skills: it_offers_count += 1
        
        # 3. Store normalized data for later accurate company searching
        match_list.append({
            "comp_normalized": company.lower().strip() if company else "",
            "title_normalized": title.lower() if title else "",
            "desc_normalized": desc_text.lower() if desc_text else ""
        })

    # 4. Resolve accurate company counts (Top 15 names, but counts include mentions)
    top15_names = [name for name, _ in company_field_counter.most_common(15)]
    top_companies_resolved = []
    
    for name in top15_names:
        lower_name = name.lower()
        accurate_count = 0
        for m in match_list:
            if (lower_name in m["comp_normalized"] or 
                lower_name in m["title_normalized"] or 
                lower_name in m["desc_normalized"]):
                accurate_count += 1
        top_companies_resolved.append({"name": name, "count": accurate_count})
    
    top_companies_resolved.sort(key=lambda x: x["count"], reverse=True)

    def format_counter(counter, limit=15):
        return [{"name": name, "count": count} for name, count in counter.most_common(limit)]

    res = TechStats(
        top_languages=format_counter(lang_counter),
        top_frameworks=format_counter(fw_counter),
        top_tools=format_counter(tool_counter),
        top_certifications=format_counter(cert_counter),
        top_methodologies=format_counter(method_counter),
        total_it_offers=it_offers_count,
        total_offers=total_offers,
        top_departments=format_counter(dept_counter, 10),
        top_companies=top_companies_resolved,
        top_categories=format_counter(cat_counter, 10)
    )
    global_stats_cache.set(cache_key, res)
    return res


@router.get("/stats/timeline")
async def get_timeline_stats(
    scale: str = Query("month", enum=["year", "month", "week", "day"]),
    db: Session = Depends(get_db)
):
    """Get offer counts grouped by period for the timeline chart. Optimized for speed and historical data."""
    cache_key = f"timeline_stats_{scale}"
    cached = global_stats_cache.get(cache_key)
    if cached:
        return cached
    try:
        # Scale mapping: how far back to look
        days_map = {
            "year": 365 * 10,  # 10 years for annual view
            "month": 365 * 5,   # 5 years for monthly view
            "week": 365 * 1,    # 1 year for weekly view
            "day": 90           # 3 months for daily view
        }
        days_back = days_map.get(scale, 90)
        cutoff = datetime.utcnow() - timedelta(days=days_back)

        # We don't use _base_query here because it's limited to 90 days.
        # This allows the timeline to show historical trends.
        query = db.query(Offer).filter(
            Offer.is_school == False,
            Offer.is_alternance == True,
            Offer.publication_date >= cutoff
        )

        engine_dialect = db.get_bind().dialect.name
        if engine_dialect == "sqlite":
            fmt = {'year': '%Y', 'week': '%Y-%W', 'day': '%Y-%m-%d'}.get(scale, '%Y-%m')
            group_expr = func.strftime(fmt, Offer.publication_date)
        else:
            # Postgres
            fmt = {'year': 'YYYY', 'week': 'IYYY-IW', 'day': 'YYYY-MM-DD'}.get(scale, 'YYYY-MM')
            group_expr = func.to_char(Offer.publication_date, fmt)

        results = query.with_entities(
            group_expr.label("period"),
            func.count(Offer.id).label("count")
        ).group_by(group_expr).order_by(group_expr).all()

        res = [{"period": r.period, "count": r.count} for r in results if r.period]
        global_stats_cache.set(cache_key, res)
        return res
    except Exception as e:
        print(f"Error in get_timeline_stats: {e}")
        return []


# ═══════════════════════════════════════════════════════
# SCRAPING ENDPOINTS
# ═══════════════════════════════════════════════════════

# Global scraping state to provide progress updates parsing via UI
global_scraping_status = {
    "is_running": False,
    "progress": 0,
    "message": "En attente",
    "details": "",
}

@router.get("/scrape/status")
async def get_scrape_status():
    """Get the current background scraping status."""
    return global_scraping_status

async def run_global_scrape():
    """Logic for full system scrape, used by API and Scheduler."""
    from scrapers import (
        LaBonneAlternanceScraper, FranceTravailScraper,
        LinkedInScraper, HelloWorkScraper, WelcomeToTheJungleScraper,
        ApecScraper, MeteojobScraper, RHAlternanceScraper
    )
    from database import SessionLocal
    import asyncio

    scrapers_list = [
        ("labonnealternance", LaBonneAlternanceScraper),
        ("francetravail", FranceTravailScraper),
        ("linkedin", LinkedInScraper),
        ("hellowork", HelloWorkScraper),
        ("wttj", WelcomeToTheJungleScraper),
        ("apec", ApecScraper),
        ("meteojob", MeteojobScraper),
        ("rhalternance", RHAlternanceScraper),
    ]

    global global_scraping_status
    if global_scraping_status["is_running"]:
        return
    
    global_scraping_status["is_running"] = True
    global_scraping_status["progress"] = 5
    global_scraping_status["message"] = "Lancement en parallèle..."
    global_scraping_status["details"] = "Démarrage des scrapers simultanés"
    
    total = len(scrapers_list)
    completed = 0
    
    async def scrape_and_save(source_name, scraper_class):
        nonlocal completed
        bg_db = SessionLocal()
        start_time = datetime.now(timezone.utc)
        try:
            # Total offers before this source's scrape
            total_before = bg_db.query(Offer).count()
            
            scraper = scraper_class()
            offers = await scraper.run()
            
            new_count = 0
            for offer_data in offers:
                # Do not save blocked offers into the database
                if offer_data.get("is_school") or offer_data.get("is_alternance") is False:
                    continue
                    
                existing = None
                if offer_data.get("source_id"):
                    existing = bg_db.query(Offer).filter(
                        Offer.source_id == offer_data["source_id"]
                    ).first()
                
                if not existing:
                    # Content-based duplicate check (Title + Company + Dept)
                    # Use exact matches for indexed fields to keep it fast
                    existing = bg_db.query(Offer).filter(
                        Offer.title == offer_data.get("title"),
                        Offer.company == offer_data.get("company"),
                        Offer.department == offer_data.get("department")
                    ).first()
                
                if not existing and offer_data.get("description"):
                    # Fallback check on exact description match
                    existing = bg_db.query(Offer).filter(
                        Offer.description == offer_data["description"]
                    ).first()
                if not existing:
                    offer = Offer(**offer_data)
                    bg_db.add(offer)
                    new_count += 1
                else:
                    if offer_data.get("description"):
                        if not existing.description or len(offer_data["description"]) > len(existing.description):
                            existing.description = offer_data["description"]
                    if offer_data.get("publication_date"):
                        existing.publication_date = offer_data["publication_date"]
                    # Update company name if old one was generic
                    if offer_data.get("company") and "confidentielle" in (existing.company or "").lower():
                        if "confidentielle" not in offer_data["company"].lower():
                            existing.company = offer_data["company"]
                bg_db.commit()
            
            # Total offers after this source's scrape
            total_after = bg_db.query(Offer).count()
            
            # Record log
            log = ScrapingLog(
                source=source_name,
                timestamp=start_time,
                offers_found=len(offers),
                offers_new=new_count,
                total_before=total_before,
                total_after=total_after,
                status="success"
            )
            bg_db.add(log)
            bg_db.commit()
            
            print(f"Scraping completed for {source_name}. {new_count} new offers added.")
        except Exception as e:
            bg_db.rollback()
            # Record error log
            try:
                error_log = ScrapingLog(
                    source=source_name,
                    timestamp=start_time,
                    status="error",
                    message=str(e)
                )
                bg_db.add(error_log)
                bg_db.commit()
            except:
                pass
            print(f"Scraping error for {source_name}: {e}")
        finally:
            bg_db.close()
            completed += 1
            prog = int((completed / total) * 95) + 5
            global_scraping_status["progress"] = prog
            global_scraping_status["message"] = f"Analyse en cours ({completed}/{total})"
            global_scraping_status["details"] = f"{source_name} terminé"

    tasks = [scrape_and_save(name, cls) for name, cls in scrapers_list]
    await asyncio.gather(*tasks, return_exceptions=True)
            
    global_stats_cache.clear()
    global_scraping_status["progress"] = 100
    global_scraping_status["message"] = "Terminé"
    global_scraping_status["details"] = "Tous les sites ont été analysés."
    global_scraping_status["is_running"] = False


@router.post("/scrape/{source}", response_model=ScrapingStatus)
async def trigger_scrape(source: str, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    """Manually trigger scraping for a specific source."""
    from scrapers import (
        LaBonneAlternanceScraper, FranceTravailScraper,
        LinkedInScraper, HelloWorkScraper, WelcomeToTheJungleScraper,
        ApecScraper, MeteojobScraper
    )

    scrapers = {
        "labonnealternance": LaBonneAlternanceScraper,
        "francetravail": FranceTravailScraper,
        "linkedin": LinkedInScraper,
        "hellowork": HelloWorkScraper,
        "wttj": WelcomeToTheJungleScraper,
        "apec": ApecScraper,
        "meteojob": MeteojobScraper,
        "rhalternance": RHAlternanceScraper,
    }

    if source not in scrapers:
        return ScrapingStatus(
            source=source,
            status="error",
            message=f"Unknown source. Available: {list(scrapers.keys())}",
            offers_found=0,
            offers_new=0
        )

    # We must use a new session for the background task
    from database import SessionLocal
    
    async def run_scraper_bg(source_name, scraper_class):
        global global_scraping_status
        if global_scraping_status["is_running"]:
            return  # Prevent concurrent scrapes
        global_scraping_status["is_running"] = True
        global_scraping_status["progress"] = 10
        global_scraping_status["message"] = f"Scraping {source_name} en cours..."
        global_scraping_status["details"] = f"Lancement de {source_name}"
        bg_db = SessionLocal()
        start_time = datetime.now(timezone.utc)
        try:
            # Total offers before
            total_before = bg_db.query(Offer).count()
            
            scraper = scraper_class()
            global_scraping_status["progress"] = 30
            offers = await scraper.run()
            global_scraping_status["progress"] = 70
            global_scraping_status["details"] = f"Enregistrement de {len(offers)} offres..."
            new_count = 0
            for offer_data in offers:
                # Do not save blocked offers into the database
                if offer_data.get("is_school") or offer_data.get("is_alternance") is False:
                    continue
                    
                existing = None
                if offer_data.get("source_id"):
                    existing = bg_db.query(Offer).filter(
                        Offer.source_id == offer_data["source_id"]
                    ).first()
                
                if not existing:
                    # Content-based duplicate check (Title + Company + Dept)
                    existing = bg_db.query(Offer).filter(
                        Offer.title == offer_data.get("title"),
                        Offer.company == offer_data.get("company"),
                        Offer.department == offer_data.get("department")
                    ).first()
                
                if not existing and offer_data.get("description"):
                    # Fallback check on exact description match
                    existing = bg_db.query(Offer).filter(
                        Offer.description == offer_data["description"]
                    ).first()
                if not existing:
                    offer = Offer(**offer_data)
                    bg_db.add(offer)
                    new_count += 1
                else:
                    if offer_data.get("description"):
                        if not existing.description or len(offer_data["description"]) > len(existing.description):
                            existing.description = offer_data["description"]
                    if offer_data.get("publication_date"):
                        existing.publication_date = offer_data["publication_date"]
                    # Update company name if old one was generic
                    if offer_data.get("company") and "confidentielle" in (existing.company or "").lower():
                        if "confidentielle" not in offer_data["company"].lower():
                            existing.company = offer_data["company"]
            bg_db.commit()
            
            # Total offers after
            total_after = bg_db.query(Offer).count()
            
            # Record log
            log = ScrapingLog(
                source=source_name,
                timestamp=start_time,
                offers_found=len(offers),
                offers_new=new_count,
                total_before=total_before,
                total_after=total_after,
                status="success"
            )
            bg_db.add(log)
            bg_db.commit()
            
            global_stats_cache.clear()
            global_scraping_status["progress"] = 100
            global_scraping_status["message"] = "Terminé"
            global_scraping_status["details"] = f"Scraping terminé pour {source_name}. {new_count} nouvelles offres ajoutées."
            print(f"Scraping completed for {source_name}. {new_count} new offers added.")
        except Exception as e:
            bg_db.rollback()
            try:
                error_log = ScrapingLog(
                    source=source_name,
                    timestamp=start_time,
                    status="error",
                    message=str(e)
                )
                bg_db.add(error_log)
                bg_db.commit()
            except:
                pass
            global_scraping_status["message"] = "Erreur"
            global_scraping_status["details"] = str(e)
            print(f"Scraping error for {source_name}: {e}")
        finally:
            bg_db.close()
            global_scraping_status["is_running"] = False

    background_tasks.add_task(run_scraper_bg, source, scrapers[source])

    return ScrapingStatus(
        source=source,
        status="started",
        offers_found=0,
        offers_new=0,
        message="Le scraping a démarré en tâche de fond.",
    )


@router.post("/scrape", response_model=list[ScrapingStatus])
async def trigger_scrape_all(background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    """Trigger scraping for all sources."""
    background_tasks.add_task(run_global_scrape)

    # Return starting status for UI
    from scrapers import (
        LaBonneAlternanceScraper, FranceTravailScraper,
        LinkedInScraper, HelloWorkScraper, WelcomeToTheJungleScraper,
        ApecScraper, MeteojobScraper
    )
    scrapers_list = ["labonnealternance", "francetravail", "linkedin", "hellowork", "wttj", "apec", "meteojob"]
    
    results = []
    for source_name in scrapers_list:
        results.append(ScrapingStatus(
            source=source_name,
            status="started",
            offers_found=0,
            offers_new=0,
            message="Le scraping a démarré en tâche de fond."
        ))

    return results


@router.post("/admin/fix-dates")
async def fix_missing_dates(db: Session = Depends(get_db)):
    """Backfill publication_date with scraped_at for offers that have no date."""
    updated = (
        db.query(Offer)
        .filter(Offer.publication_date.is_(None), Offer.scraped_at.isnot(None))
        .update({Offer.publication_date: Offer.scraped_at}, synchronize_session=False)
    )
    db.commit()
    return {"updated": updated, "message": f"{updated} offres mises à jour avec leur date de scraping."}


@router.post("/admin/fix-schools")
async def fix_school_flags(db: Session = Depends(get_db)):
    """Re-scan all offers and flag school offers that slipped through."""
    from scrapers.utils import is_school_offer

    # Get all offers not yet flagged as school
    offers = db.query(Offer).filter(Offer.is_school == False).all()  # noqa: E712
    flagged = 0
    flagged_names = []

    for offer in offers:
        if is_school_offer(offer.company or "", offer.description or ""):
            offer.is_school = True
            flagged += 1
            flagged_names.append(offer.company)

    db.commit()

    # Get unique school names that were flagged
    unique_schools = sorted(set(flagged_names))

    return {
        "flagged": flagged,
        "unique_schools": unique_schools[:50],
        "message": f"{flagged} offres marquées comme écoles.",
    }


@router.post("/admin/fix-alternance")
async def fix_alternance_flags(db: Session = Depends(get_db)):
    """Re-scan all offers and flag non-alternance offers (CDIs) that slipped through."""
    from scrapers.skills_extractor import is_alternance_offer

    # Get all offers currently marked as alternance
    offers = db.query(Offer).filter(Offer.is_alternance == True).all()  # noqa: E712
    flagged = 0
    flagged_titles = []

    for offer in offers:
        if not is_alternance_offer(offer.title or "", offer.description or "", offer.contract_type):
            offer.is_alternance = False
            flagged += 1
            flagged_titles.append(offer.title)

    db.commit()

    return {
        "flagged": flagged,
        "sample_titles": list(set(flagged_titles))[:20],
        "message": f"{flagged} offres marquées comme non-alternance (CDI/CDD).",
    }


@router.post("/admin/cleanup-duplicates")
async def cleanup_duplicates(db: Session = Depends(get_db)):
    """Remove existing duplicate offers based on title, description, location, and department."""
    # This identifies duplicates and keeps the one with the most recent publication or scrap date.
    all_offers = db.query(Offer).order_by(desc(Offer.scraped_at)).all()
    
    seen = set()
    to_delete = []
    
    for offer in all_offers:
        # Create a unique key for comparison
        # We normalize slightly to be safer (strip and lower)
        key = (
            (offer.title or "").strip().lower(),
            (offer.description or "").strip().lower(),
            (offer.location or "").strip().lower(),
            (offer.department or "").strip().lower()
        )
        
        if key in seen:
            to_delete.append(offer.id)
        else:
            seen.add(key)
    
    deleted_count = 0
    if to_delete:
        # Delete in chunks to avoid large query issues
        chunk_size = 500
        for i in range(0, len(to_delete), chunk_size):
            chunk = to_delete[i:i+chunk_size]
            db.query(Offer).filter(Offer.id.in_(chunk)).delete(synchronize_session=False)
            deleted_count += len(chunk)
            
    db.commit()
    return {"deleted": deleted_count, "message": f"{deleted_count} offres en doublon ont été supprimées."}


@router.post("/admin/fix-urls")
async def fix_missing_urls(db: Session = Depends(get_db)):
    """Rebuild missing URLs for La Bonne Alternance offers using their source_id."""
    # 1. Update standard matcha/peJob that are completely missing URLs
    offers = db.query(Offer).filter(
        Offer.source == "labonnealternance",
        (Offer.url == None) | (Offer.url == "")  # noqa: E711
    ).all()
    
    updated = 0
    for offer in offers:
        if offer.source_id and offer.source_id.startswith("lba_"):
            parts = offer.source_id.split("_")
            if len(parts) >= 3:
                offer_id = parts[-1]
                idea_type = "_".join(parts[1:-1])
                
                if idea_type == "matcha":
                    offer.url = f"https://labonnealternance.apprentissage.beta.gouv.fr/recherche-apprentissage?display=list&page=fiche&type=matcha&itemId={offer_id}"
                    updated += 1
                elif idea_type == "peJob":
                    offer.url = f"https://candidat.francetravail.fr/offres/recherche/detail/{offer_id}"
                    updated += 1

    # 2. Revert broken "partner" or "partnerJob" fallbacks to None, 
    # since LBA frontend shows an empty page for them.
    broken_offers = db.query(Offer).filter(
        Offer.source == "labonnealternance",
        (Offer.url.like("%type=partner%"))
    ).all()
    
    for offer in broken_offers:
        offer.url = None
        updated += 1
                
    db.commit()
    return {"updated": updated, "message": f"{updated} URLs corrigées ou retirées pour La Bonne Alternance."}

