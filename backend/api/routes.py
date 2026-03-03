"""API routes for the alternance dashboard."""

import json
from collections import Counter
from datetime import datetime, timedelta
from typing import Optional
from fastapi import APIRouter, Depends, Query, HTTPException, status, BackgroundTasks
from sqlalchemy.orm import Session
from sqlalchemy import func, or_, desc

from database import get_db
from models import Offer, User, Favorite
from schemas import (
    OfferListResponse, OfferResponse, FilterOptions, ScrapingStatus, TechStats,
    UserRegister, UserLogin, UserResponse, TokenResponse,
    FavoriteCreate, FavoriteUpdate, FavoriteResponse,
)
from auth import hash_password, verify_password, create_token, get_current_user, get_optional_user

router = APIRouter(prefix="/api", tags=["offers"])


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
    """Base query: exclude school offers, non-alternance CDD, and specific schools."""
    return db.query(Offer).filter(
        Offer.is_school == False,  # noqa: E712
        Offer.is_alternance == True,  # noqa: E712
        # Exclude known training schools / "fake" company offers
        Offer.company.notilike("%iscod%"),
        Offer.company.notilike("%icademie%"),
        Offer.company.notilike("%i-cademie%"),
        Offer.company.notilike("%livecampus%"),
        Offer.company.notilike("%live campus%"),
        Offer.company.notilike("%aurlom%"),
        Offer.company.notilike("%2i academy%"),
        Offer.company.notilike("%openclassrooms%"),
        Offer.company.notilike("%studi%"),
        Offer.company.notilike("%doranco%"),
        Offer.company.notilike("%ifocop%"),
        Offer.company.notilike("%pigier%"),
        Offer.company.notilike("%aftec%"),
        Offer.company.notilike("%enaco%"),
        Offer.company.notilike("%esecad%"),
        Offer.company.notilike("%visiplus%"),
        Offer.company.notilike("%digital campus%"),
        Offer.company.notilike("%wild code school%"),
        Offer.company.notilike("%simplon%"),
        Offer.company.notilike("%rocket school%"),
        Offer.company.notilike("%pop school%"),
        Offer.company.notilike("%village de l'emploi%"),
        Offer.company.notilike("%village emploi%"),
        Offer.company.notilike("%groupe alternance%"),
        Offer.company.notilike("%alternance azur%"),
        Offer.company.notilike("%mydigitalschool%"),
        Offer.company.notilike("%my digital school%"),
        Offer.company.notilike("%ynov%"),
        Offer.company.notilike("%mbway%"),
        Offer.company.notilike("%isefac%"),
        Offer.company.notilike("%bachelor institute%"),
        Offer.company.notilike("%maestris%"),
        Offer.company.notilike("%efficom%"),
        Offer.company.notilike("%epsi%"),
        Offer.company.notilike("%next-u%"),
        Offer.company.notilike("%nextu%"),
        Offer.company.notilike("%sup de vinci%"),
        Offer.company.notilike("%esgi%"),
        Offer.company.notilike("%hetic%"),
        Offer.company.notilike("%holberton%"),
        Offer.company.notilike("%fitec%"),
        Offer.company.notilike("%diginamic%"),
        Offer.company.notilike("%edugroupe%"),
        Offer.company.notilike("%human booster%"),
        Offer.company.notilike("%cogefi%"),
        Offer.company.notilike("%3w academy%"),
        Offer.company.notilike("%o'clock%"),
        Offer.company.notilike("%la capsule%"),
        Offer.company.notilike("%le wagon%"),
        Offer.company.notilike("%ada tech school%"),
        Offer.company.notilike("%cfa %"),
        Offer.company.notilike("%estiam%"),
        Offer.company.notilike("%itescia%"),
        Offer.company.notilike("%ingetis%"),
        Offer.company.notilike("%groupe igs%"),
        Offer.company.notilike("%sciences-u%"),
        Offer.company.notilike("%sciences u%"),
        Offer.company.notilike("%ecema%"),
        Offer.company.notilike("%skill and you%"),
        Offer.company.notilike("%scholia%"),
        Offer.company.notilike("%livementor%"),
        Offer.company.notilike("%cybersup%"),
        Offer.company.notilike("%stephenson formation%"),
        Offer.company.notilike("%avlis formation%"),
        Offer.company.notilike("%stand up formation%"),
        Offer.company.notilike("%pro-fyl%"),
        Offer.company.notilike("%arefip%"),
        Offer.company.notilike("%ima business school%"),
        Offer.company.notilike("%cfi formation%"),
        Offer.company.notilike("%cma formation%"),
        Offer.company.notilike("%directt formation%"),
        Offer.company.notilike("%nexa digital%"),
        Offer.company.notilike("%formation alternance superieure%"),
        Offer.company.notilike("%organisation formations informatiques%"),
    )


@router.get("/offers", response_model=OfferListResponse)
async def get_offers(
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
    keyword: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
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
        query = query.filter(Offer.category.ilike(f"%{category}%"))
    if location:
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
                    Offer.profile == profile,
                    Offer.description.ilike("%bac+3%"),
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
                    Offer.profile == profile,
                    Offer.description.ilike("%bac+4%"),
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
                    Offer.profile == profile,
                    Offer.description.ilike("%bac+5%"),
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
                    Offer.profile == profile,
                    Offer.description.ilike("%bac+2%"),
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
        query = query.filter(Offer.source == source)
    if technology:
        query = query.filter(Offer.skills_all.ilike(f"%{technology}%"))

    # Date filters
    if date_filter:
        now = datetime.utcnow()
        if date_filter == "today":
            query = query.filter(Offer.publication_date >= now.replace(hour=0, minute=0, second=0))
        elif date_filter == "week":
            query = query.filter(Offer.publication_date >= now - timedelta(days=7))
        elif date_filter == "month":
            query = query.filter(Offer.publication_date >= now - timedelta(days=30))
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
async def get_filter_options(db: Session = Depends(get_db)):
    """Get available filter options based on current data."""
    base_query = _base_query(db)

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
    counter = Counter()
    results = query.with_entities(Offer.skills_all).filter(
        Offer.skills_all.isnot(None), Offer.skills_all != "[]"
    ).all()
    for (skills_json,) in results:
        try:
            skills = json.loads(skills_json)
            if isinstance(skills, list):
                counter.update(skills)
        except (json.JSONDecodeError, TypeError):
            pass
    return [tech for tech, _ in counter.most_common(50)]


@router.get("/stats")
async def get_stats(db: Session = Depends(get_db)):
    """Get dashboard statistics."""
    base_query = _base_query(db)
    total_offers = base_query.count()

    by_source = dict(
        base_query.with_entities(Offer.source, func.count(Offer.id))
        .group_by(Offer.source).all()
    )
    by_category = dict(
        base_query.with_entities(Offer.category, func.count(Offer.id))
        .filter(Offer.category.isnot(None), Offer.category != "")
        .group_by(Offer.category)
        .order_by(desc(func.count(Offer.id)))
        .limit(10).all()
    )
    recent = base_query.filter(
        Offer.publication_date >= datetime.utcnow() - timedelta(hours=24)
    ).count()
    it_offers = base_query.filter(
        Offer.skills_all.isnot(None),
        Offer.skills_all != "[]",
    ).count()

    bac2_offers = base_query.filter(
        or_(
            Offer.profile.ilike("%bac+2%"),
            Offer.description.ilike("%bac+2%"),
            Offer.description.ilike("%bac + 2%"),
            Offer.description.ilike("%bts%"),
            Offer.description.ilike("%dut%"),
            Offer.title.ilike("%bac+2%"),
            Offer.title.ilike("%bac + 2%"),
            Offer.title.ilike("%bts%"),
            Offer.title.ilike("%dut%")
        )
    ).count()

    bac3_offers = base_query.filter(
        or_(
            Offer.profile.ilike("%bac+3%"),
            Offer.description.ilike("%bac+3%"),
            Offer.description.ilike("%bac + 3%"),
            Offer.description.ilike("%licence%"),
            Offer.description.ilike("%bachelor%"),
            Offer.title.ilike("%bac+3%"),
            Offer.title.ilike("%bac + 3%"),
            Offer.title.ilike("%licence%"),
            Offer.title.ilike("%bachelor%")
        )
    ).count()

    bac4_offers = base_query.filter(
        or_(
            Offer.profile.ilike("%bac+4%"),
            Offer.description.ilike("%bac+4%"),
            Offer.description.ilike("%bac + 4%"),
            Offer.description.ilike("%m1%"),
            Offer.description.ilike("%maîtrise%"),
            Offer.title.ilike("%bac+4%"),
            Offer.title.ilike("%bac + 4%"),
            Offer.title.ilike("%m1%")
        )
    ).count()

    bac5_offers = base_query.filter(
        or_(
            Offer.profile.ilike("%bac+5%"),
            Offer.description.ilike("%bac+5%"),
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
    ).count()

    return {
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


@router.get("/stats/tech", response_model=TechStats)
async def get_tech_stats(db: Session = Depends(get_db)):
    """Get detailed technology statistics."""
    base_query = _base_query(db)
    total_offers = base_query.count()

    lang_counter = Counter()
    fw_counter = Counter()
    tool_counter = Counter()
    cert_counter = Counter()
    method_counter = Counter()

    results = base_query.with_entities(
        Offer.skills_languages,
        Offer.skills_frameworks,
        Offer.skills_tools,
        Offer.skills_certifications,
        Offer.skills_methodologies,
    ).filter(
        Offer.skills_all.isnot(None),
        Offer.skills_all != "[]",
    ).all()

    total_it = len(results)

    for langs, fws, tools, certs, methods in results:
        for data, counter in [
            (langs, lang_counter),
            (fws, fw_counter),
            (tools, tool_counter),
            (certs, cert_counter),
            (methods, method_counter),
        ]:
            if data:
                try:
                    items = json.loads(data)
                    if isinstance(items, list):
                        counter.update(items)
                except (json.JSONDecodeError, TypeError):
                    pass

    # Compute additional interesting global stats for cards
    top_companies_query = base_query.with_entities(Offer.company, func.count(Offer.id))\
        .filter(Offer.company.isnot(None), Offer.company != "")\
        .filter(Offer.company.notilike("%confidentiel%"))\
        .group_by(Offer.company)\
        .order_by(desc(func.count(Offer.id)))\
        .limit(13).all()
    
    top_departments_query = base_query.with_entities(Offer.department, func.count(Offer.id))\
        .filter(Offer.department.isnot(None), Offer.department != "")\
        .group_by(Offer.department)\
        .order_by(desc(func.count(Offer.id)))\
        .limit(10).all()

    top_categories_query = base_query.with_entities(Offer.category, func.count(Offer.id))\
        .filter(Offer.category.isnot(None), Offer.category != "")\
        .filter(Offer.category.notilike("%autre%"))\
        .filter(Offer.category.notin_(["Développement, IT", "Technologies de l'Information"]))\
        .group_by(Offer.category)\
        .order_by(desc(func.count(Offer.id)))\
        .limit(10).all()

    def to_list(counter: Counter, limit: int = 15) -> list[dict]:
        return [{"name": name, "count": count} for name, count in counter.most_common(limit)]
    
    def format_query(q) -> list[dict]:
        return [{"name": str(name), "count": count} for name, count in q]

    return TechStats(
        top_languages=to_list(lang_counter),
        top_frameworks=to_list(fw_counter),
        top_tools=to_list(tool_counter),
        top_certifications=to_list(cert_counter),
        top_methodologies=to_list(method_counter),
        total_it_offers=total_it,
        total_offers=total_offers,
        top_departments=format_query(top_departments_query),
        top_companies=format_query(top_companies_query),
        top_categories=format_query(top_categories_query)
    )


@router.get("/stats/timeline")
async def get_timeline_stats(db: Session = Depends(get_db)):
    """Get offer counts grouped by month for the timeline chart (up to 10 years back)."""
    base_query = _base_query(db)
    cutoff = datetime.utcnow() - timedelta(days=365 * 10)

    month_expr = func.to_char(Offer.publication_date, 'YYYY-MM')

    results = (
        base_query
        .with_entities(
            month_expr.label("month"),
            func.count(Offer.id).label("count"),
        )
        .filter(
            Offer.publication_date.isnot(None),
            Offer.publication_date >= cutoff,
        )
        .group_by(month_expr)
        .order_by(month_expr)
        .all()
    )

    return [{"month": row.month, "count": row.count} for row in results]


# ═══════════════════════════════════════════════════════
# SCRAPING ENDPOINTS
# ═══════════════════════════════════════════════════════

@router.post("/scrape/{source}", response_model=ScrapingStatus)
async def trigger_scrape(source: str, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    """Manually trigger scraping for a specific source."""
    from scrapers import (
        LaBonneAlternanceScraper, FranceTravailScraper,
        LinkedInScraper, HelloWorkScraper, WelcomeToTheJungleScraper
    )

    scrapers = {
        "labonnealternance": LaBonneAlternanceScraper,
        "francetravail": FranceTravailScraper,
        "linkedin": LinkedInScraper,
        "hellowork": HelloWorkScraper,
        "wttj": WelcomeToTheJungleScraper,
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
        bg_db = SessionLocal()
        try:
            scraper = scraper_class()
            offers = await scraper.run()
            new_count = 0
            for offer_data in offers:
                existing = None
                if offer_data.get("source_id"):
                    existing = bg_db.query(Offer).filter(
                        Offer.source_id == offer_data["source_id"]
                    ).first()
                if not existing:
                    offer = Offer(**offer_data)
                    bg_db.add(offer)
                    new_count += 1
                else:
                    if not existing.description and offer_data.get("description"):
                        existing.description = offer_data["description"]
                    if not existing.publication_date and offer_data.get("publication_date"):
                        existing.publication_date = offer_data["publication_date"]
            bg_db.commit()
            print(f"Scraping completed for {source_name}. {new_count} new offers added.")
        except Exception as e:
            bg_db.rollback()
            print(f"Scraping error for {source_name}: {e}")
        finally:
            bg_db.close()

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
    from scrapers import (
        LaBonneAlternanceScraper, FranceTravailScraper,
        LinkedInScraper, HelloWorkScraper, WelcomeToTheJungleScraper
    )

    scrapers_list = [
        ("labonnealternance", LaBonneAlternanceScraper),
        ("francetravail", FranceTravailScraper),
        ("linkedin", LinkedInScraper),
        ("hellowork", HelloWorkScraper),
        ("wttj", WelcomeToTheJungleScraper),
    ]

    from database import SessionLocal

    async def run_all_scrapers_bg():
        for source_name, scraper_class in scrapers_list:
            bg_db = SessionLocal()
            try:
                scraper = scraper_class()
                offers = await scraper.run()
                new_count = 0
                for offer_data in offers:
                    existing = None
                    if offer_data.get("source_id"):
                        existing = bg_db.query(Offer).filter(
                            Offer.source_id == offer_data["source_id"]
                        ).first()
                    if not existing:
                        offer = Offer(**offer_data)
                        bg_db.add(offer)
                        new_count += 1
                    else:
                        if not existing.description and offer_data.get("description"):
                            existing.description = offer_data["description"]
                        if not existing.publication_date and offer_data.get("publication_date"):
                            existing.publication_date = offer_data["publication_date"]
                bg_db.commit()
                print(f"Scraping completed for {source_name}. {new_count} new offers added.")
            except Exception as e:
                bg_db.rollback()
                print(f"Scraping error for {source_name}: {e}")
            finally:
                bg_db.close()

    background_tasks.add_task(run_all_scrapers_bg)

    results = []
    for source_name, _ in scrapers_list:
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
