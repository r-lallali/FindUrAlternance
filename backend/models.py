"""SQLAlchemy models for the alternance dashboard."""

import uuid
from datetime import datetime, timezone
from sqlalchemy import Column, String, Text, Boolean, DateTime, Index, ForeignKey, Integer
from sqlalchemy.orm import relationship
from database import Base


class ScrapingLog(Base):
    """Log of a scraping session."""

    __tablename__ = "scraping_logs"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    source = Column(String(100), nullable=False)
    timestamp = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    offers_found = Column(Integer, default=0)
    offers_new = Column(Integer, default=0)
    total_before = Column(Integer, default=0)
    total_after = Column(Integer, default=0)
    duration_seconds = Column(Integer, nullable=True)
    status = Column(String(20), nullable=False, default="success")  # success | error
    message = Column(Text, nullable=True)

    def __repr__(self):
        return f"<ScrapingLog {self.source} @ {self.timestamp}: {self.offers_new} new>"


class Offer(Base):
    """Model representing an alternance job offer."""

    __tablename__ = "offers"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    title = Column(String(500), nullable=False, index=True)
    company = Column(String(300), nullable=False, index=True)
    location = Column(String(300), nullable=True)
    department = Column(String(10), nullable=True)
    contract_type = Column(String(100), nullable=True)
    salary = Column(String(200), nullable=True)
    salary_min = Column(String(50), nullable=True)
    salary_max = Column(String(50), nullable=True)
    description = Column(Text, nullable=True)
    profile = Column(String(500), nullable=True)
    category = Column(String(200), nullable=True)

    # IT Skills & Technologies (stored as JSON lists)
    skills_languages = Column(Text, nullable=True)
    skills_frameworks = Column(Text, nullable=True)
    skills_tools = Column(Text, nullable=True)
    skills_certifications = Column(Text, nullable=True)
    skills_methodologies = Column(Text, nullable=True)
    skills_all = Column(Text, nullable=True)

    publication_date = Column(DateTime, nullable=True, index=True)
    source = Column(String(100), nullable=False)
    url = Column(String(1000), nullable=True)
    source_id = Column(String(500), nullable=True, unique=True)
    is_school = Column(Boolean, default=False)
    is_alternance = Column(Boolean, default=True)
    is_active = Column(Boolean, default=True, nullable=False)
    scraped_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    last_seen_at = Column(DateTime, nullable=True)

    favorites = relationship("Favorite", back_populates="offer", cascade="all, delete-orphan")

    __table_args__ = (
        Index("idx_source_date", "source", "publication_date"),
        Index("idx_school_date", "is_school", "publication_date"),
        Index("idx_alternance", "is_alternance"),
    )

    def __repr__(self):
        return f"<Offer {self.title} @ {self.company}>"


class User(Base):
    """User account."""

    __tablename__ = "users"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    username = Column(String(50), nullable=False, unique=True, index=True)
    email = Column(String(255), nullable=False, unique=True, index=True)
    password_hash = Column(String(255), nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    favorites = relationship("Favorite", back_populates="user", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<User {self.username}>"


class Favorite(Base):
    """User's saved offer with application status."""

    __tablename__ = "favorites"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = Column(String(36), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    offer_id = Column(String(36), ForeignKey("offers.id", ondelete="CASCADE"), nullable=False)
    status = Column(String(20), nullable=False, default="to_apply")  # to_apply | applied | rejected
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))

    user = relationship("User", back_populates="favorites")
    offer = relationship("Offer", back_populates="favorites")

    __table_args__ = (
        Index("idx_user_offer", "user_id", "offer_id", unique=True),
        Index("idx_user_status", "user_id", "status"),
    )

    def __repr__(self):
        return f"<Favorite user={self.user_id} offer={self.offer_id} status={self.status}>"
