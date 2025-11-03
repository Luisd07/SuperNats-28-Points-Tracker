from __future__ import annotations
from datetime import datetime, date
from typing import List, Optional

from sqlalchemy import (
    String, Integer, BigInteger, Date, DateTime, Enum, Text, ForeignKey,
    UniqueConstraint, CheckConstraint, Index, func, Boolean, text
)
from sqlalchemy.orm import relationship, DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass

# --- Enums ---
SessionTypeEnum = Enum("Practice", "Qualifying", "Heat", "Prefinal","Final", name="session_type_enum")
BasisEnum = Enum("provisional", "official", name="basis_enum")
ResultStatusEnum = Enum("FINISH", "DNF", "DQ", "DNS", name="result_status_enum")
PenaltyTypeEnum = Enum("TIME", "POSITION", "DQ", "LAP_INVALID", name="penalty_type_enum")
SessionStatusEnum = Enum("live", "provisional", "official", "cancelled", name="session_status_enum")

# --- Core ---
class Driver(Base):
    __tablename__ = "drivers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    transponder: Mapped[Optional[str]] = mapped_column(String(64), unique=False, index=True) 
    first_name: Mapped[str] = mapped_column(String(64), nullable=False)
    last_name: Mapped[str] = mapped_column(String(64), nullable=False)
    team: Mapped[Optional[str]] = mapped_column(String(128))   
    chassis: Mapped[Optional[str]] = mapped_column(String(128)) 

    laps: Mapped[List["Lap"]] = relationship("Lap", back_populates="driver", cascade="all, delete-orphan")
    results: Mapped[List["Result"]] = relationship("Result", back_populates="driver", cascade="all, delete-orphan")
    points: Mapped[List["PointAward"]] = relationship("PointAward", back_populates="driver", cascade="all, delete-orphan")
    entries: Mapped[List["Entry"]] = relationship("Entry", back_populates="driver", cascade="all, delete-orphan")

    def __repr__(self) -> str:
        return f"<Driver {self.first_name} {self.last_name}>"

class Event(Base):
    __tablename__ = "events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    start_date: Mapped[date] = mapped_column(Date, nullable=False)
    end_date: Mapped[date] = mapped_column(Date, nullable=False)
    location: Mapped[Optional[str]] = mapped_column(String(256))

    classes: Mapped[List["RaceClass"]] = relationship("RaceClass", back_populates="event", cascade="all, delete-orphan")
    sessions: Mapped[List["Session"]] = relationship("Session", back_populates="event", cascade="all, delete-orphan")
    entries: Mapped[List["Entry"]] = relationship("Entry", back_populates="event", cascade="all, delete-orphan")


class RaceClass(Base):
    __tablename__ = "classes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    event_id: Mapped[int] = mapped_column(ForeignKey("events.id", ondelete="CASCADE"), index=True)
    name: Mapped[str] = mapped_column(String(64), nullable=False)

    event: Mapped[Event] = relationship(back_populates="classes")
    sessions: Mapped[List["Session"]] = relationship(back_populates="race_class", cascade="all, delete-orphan")
    entries: Mapped[List["Entry"]] = relationship("Entry", back_populates="race_class", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint("event_id", "name", name="uq_class_event_name"),
    )


class Session(Base):
    __tablename__ = "sessions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    event_id: Mapped[int] = mapped_column(ForeignKey("events.id", ondelete="CASCADE"), index=True)
    class_id: Mapped[int] = mapped_column(ForeignKey("classes.id", ondelete="CASCADE"), index=True)
    session_type: Mapped[str] = mapped_column(SessionTypeEnum, index=True)
    session_name: Mapped[Optional[str]] = mapped_column(String(120))
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    ended_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(SessionStatusEnum, default="live", index=True)

    event: Mapped[Event] = relationship(back_populates="sessions")
    race_class: Mapped[RaceClass] = relationship(back_populates="sessions")

    laps: Mapped[List["Lap"]] = relationship(back_populates="session", cascade="all, delete-orphan")
    results: Mapped[List["Result"]] = relationship(back_populates="session", cascade="all, delete-orphan")
    penalties: Mapped[List["Penalty"]] = relationship(back_populates="session", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint("event_id", "class_id", "session_name", name="uq_session_event_class_name"),
    )


class Entry(Base):
    __tablename__ = "entries"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    event_id: Mapped[int] = mapped_column(ForeignKey("events.id", ondelete="CASCADE"), index=True)
    class_id: Mapped[int] = mapped_column(ForeignKey("classes.id", ondelete="CASCADE"), index=True)
    driver_id: Mapped[int] = mapped_column(ForeignKey("drivers.id", ondelete="CASCADE"), index=True)

    number: Mapped[Optional[str]] = mapped_column(String(8))  
    team: Mapped[Optional[str]] = mapped_column(String(128))
    chassis: Mapped[Optional[str]] = mapped_column(String(128))
    transponder: Mapped[Optional[str]] = mapped_column(String(64))  

    event: Mapped[Event] = relationship(back_populates="entries")
    race_class: Mapped[RaceClass] = relationship(back_populates="entries")
    driver: Mapped[Driver] = relationship(back_populates="entries")

    __table_args__ = (
        UniqueConstraint("event_id", "class_id", "number", name="uq_entry_event_class_number"),
        UniqueConstraint("event_id", "class_id", "driver_id", name="uq_entry_event_class_driver"),
        Index("ix_entries_event_class_number", "event_id", "class_id", "number"),
    )


class Lap(Base):
    __tablename__ = "laps"
    __table_args__ = (
        UniqueConstraint('session_id', 'driver_id', 'lap_number', name='uq_session_driver_lap'),
        Index('ix_laps_session_driver_time', 'session_id', 'driver_id', 'lap_time_ms'),
        Index('ix_laps_session_lapno', 'session_id', 'lap_number'),
        Index('ix_laps_session_ts', 'session_id', 'timestamp'),
        {"sqlite_autoincrement": True},  # ensure AUTOINCREMENT on SQLite
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)      # INTEGER PRIMARY KEY
    session_id: Mapped[int] = mapped_column(ForeignKey("sessions.id", ondelete="CASCADE"), index=True)
    driver_id: Mapped[int] = mapped_column(ForeignKey("drivers.id", ondelete="CASCADE"), index=True)

    lap_number: Mapped[int] = mapped_column(Integer, nullable=False)
    lap_time_ms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)  # <- safer
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    is_valid: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("1"))

    session: Mapped["Session"] = relationship(back_populates="laps")
    driver: Mapped["Driver"] = relationship(back_populates="laps")


class Penalty(Base):
    __tablename__ = "penalties"

    id: Mapped[int] = mapped_column(primary_key=True)
    session_id: Mapped[int] = mapped_column(ForeignKey("sessions.id", ondelete="CASCADE"), index=True)
    driver_id: Mapped[int] = mapped_column(ForeignKey("drivers.id", ondelete="CASCADE"), index=True)
    type: Mapped[str] = mapped_column(PenaltyTypeEnum)
    lap_no: Mapped[Optional[int]] = mapped_column(Integer)
    value_ms: Mapped[Optional[int]] = mapped_column(Integer)
    value_positions: Mapped[Optional[int]] = mapped_column(Integer)
    note: Mapped[Optional[str]] = mapped_column(Text)
    source: Mapped[Optional[str]] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)

    session: Mapped[Session] = relationship(back_populates="penalties")
    driver: Mapped[Driver] = relationship()


class Result(Base):
    __tablename__ = "results"

    id: Mapped[int] = mapped_column(primary_key=True)
    session_id: Mapped[int] = mapped_column(ForeignKey("sessions.id", ondelete="CASCADE"), index=True)
    driver_id: Mapped[int] = mapped_column(ForeignKey("drivers.id", ondelete="CASCADE"), index=True)

    version: Mapped[int] = mapped_column(Integer)            
    basis: Mapped[str] = mapped_column(BasisEnum, index=True) 

    position: Mapped[Optional[int]] = mapped_column(Integer, index=True)
    best_lap_ms: Mapped[Optional[int]] = mapped_column(Integer)
    last_lap_ms: Mapped[Optional[int]] = mapped_column(Integer)
    total_time_ms: Mapped[Optional[int]] = mapped_column(BigInteger)
    gap_to_p1_ms: Mapped[Optional[int]] = mapped_column(Integer)
    status_code: Mapped[Optional[str]] = mapped_column(ResultStatusEnum)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    session: Mapped[Session] = relationship(back_populates="results")
    driver: Mapped[Driver] = relationship(back_populates="results")

    __table_args__ = (
        Index("ix_results_session_basis_version", "session_id", "basis", "version"),
        Index("ix_results_session_position", "session_id", "position"),
        UniqueConstraint("session_id", "driver_id", "basis", "version", name="uq_result_versioned_row"),
        CheckConstraint("position IS NULL OR position > 0", name="ck_result_pos_pos"),
    )


class Point(Base):
    __tablename__ = "points"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(128), nullable=False)  
    bonus_lap: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("0"))
    bonus_pole: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("0"))

    scales: Mapped[List["PointScale"]] = relationship(back_populates="point", cascade="all, delete-orphan")


class PointScale(Base):
    __tablename__ = "points_scales"

    id: Mapped[int] = mapped_column(primary_key=True)
    point_id: Mapped[int] = mapped_column(ForeignKey("points.id", ondelete="CASCADE"), index=True) 
    session_type: Mapped[str] = mapped_column(SessionTypeEnum)
    position: Mapped[int] = mapped_column(Integer)
    points: Mapped[int] = mapped_column(Integer)

    point: Mapped[Point] = relationship(back_populates="scales")

    __table_args__ = (
        UniqueConstraint("point_id", "session_type", "position", name="uq_points_scale_key"),
        CheckConstraint("position > 0", name="ck_points_pos_pos"),
    )


class PointAward(Base):
    __tablename__ = "points_awards"

    id: Mapped[int] = mapped_column(primary_key=True)
    session_id: Mapped[int] = mapped_column(ForeignKey("sessions.id", ondelete="CASCADE"), index=True)
    driver_id: Mapped[int] = mapped_column(ForeignKey("drivers.id", ondelete="CASCADE"), index=True)
    basis: Mapped[str] = mapped_column(BasisEnum, index=True)    
    version: Mapped[int] = mapped_column(Integer)

    position: Mapped[Optional[int]] = mapped_column(Integer)
    base_points: Mapped[int] = mapped_column(Integer, server_default=text("0"))
    bonus_points: Mapped[int] = mapped_column(Integer, server_default=text("0"))
    total_points: Mapped[int] = mapped_column(Integer, server_default=text("0"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    driver: Mapped[Driver] = relationship(back_populates="points")
    session: Mapped[Session] = relationship()

    __table_args__ = (
        UniqueConstraint("session_id", "driver_id", "basis", "version", name="uq_points_versioned_row"),
    )


class CurrentSession(Base):
    __tablename__ = "current_session"
    id: Mapped[int] = mapped_column(primary_key=True)
    event_id: Mapped[int] = mapped_column(ForeignKey("events.id", ondelete="CASCADE"))
    class_id: Mapped[int] = mapped_column(ForeignKey("classes.id", ondelete="CASCADE"))
    session_id: Mapped[int] = mapped_column(ForeignKey("sessions.id", ondelete="CASCADE"))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
