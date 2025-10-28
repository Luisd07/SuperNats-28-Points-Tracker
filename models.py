from __future__ import annotations
from datetime import datetime, date
from typing import List, Optional

from sqlalchemy import (
    String, Integer, BigInteger, Date, DateTime, Enum, Text, ForeignKey, UniqueConstraint, CheckConstraint, Index, func 
)
from sqlalchemy.orm import relationship, DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass

SessionTypeEnum = Enum("Practice", "Qualifying", "Heat", "Prefinal", "Final", name="session_type_enum")

BasisEnum = Enum("provisional", "official", name="basis_enum")

ResultStatusEnum = Enum("FINISH", "DNF", "DQ", "DNS", name="result_status_enum")

PenaltyTypeEnum = Enum("TIME", "POSITION", "DQ", "LAP_INVALID", name="penalty_type_enum")

class Driver(Base):
    __tablename__ = "drivers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    transponder: Mapped[Optional[str]] = mapped_column(String(64), unique=True)
    number: Mapped[Optional[int]] = mapped_column(Integer, unique=True)
    first_name: Mapped[str] = mapped_column(String(64), nullable=False)
    last_name: Mapped[str] = mapped_column(String(64), nullable=False)
    team: Mapped[Optional[str]] = mapped_column(String(128))
    chassis: Mapped[Optional[str]] = mapped_column(String(128))

    laps: Mapped[List["Lap"]] = relationship("Lap", back_populates="driver", cascade="all, delete-orphan")
    results: Mapped[List["Result"]] = relationship("Result", back_populates="driver", cascade="all, delete-orphan")
    points: Mapped[List["Point"]] = relationship("Point", back_populates="driver", cascade="all, delete-orphan")

    def __repr__(self) -> str:
        return f"<Driver {self.number or ''} {self.first_name} {self.last_name}>"


class Event(Base):
    __tablename__ = "events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    start_date: Mapped[date] = mapped_column(Date, nullable=False)
    end_date: Mapped[date] = mapped_column(Date, nullable=False)
    location: Mapped[Optional[str]] = mapped_column(String(256))

    classes: Mapped[List["RaceClass"]] = relationship("RaceClass", back_populates="event", cascade="all, delete-orphan")
    sessions: Mapped[List["Session"]] = relationship("Session", back_populates="event", cascade="all, delete-orphan")



class RaceClass(Base):
     __tablename__ = "classes"

     id: Mapped[int] = mapped_column(Integer, primary_key=True)
     event_id: Mapped[int] = mapped_column(ForeignKey("events.id", ondelete="CASCADE"), index=True)
     name: Mapped[str] = mapped_column(String(64), nullable=False)

     event: Mapped[Event] = relationship(back_populates="classes")
     sessions: Mapped[List["Session"]] = relationship(back_populates="race_class", cascade="all, delete-orphan")

     __table_args__ = (
        UniqueConstraint("event_id", "name", name="uq_class_event_name"),
    )



class Session(Base):
    __tablename__ = "sessions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    event_id: Mapped[int] = mapped_column(ForeignKey("events.id", ondelete="CASCADE"), index=True)
    class_id: Mapped[int] = mapped_column(ForeignKey("classes.id", ondelete="CASCADE"), index=True)
    session_type: Mapped[str] = mapped_column(SessionTypeEnum, index=True)
    session_name: Mapped[Optional[str]] = mapped_column(String(120))  # e.g., "HEAT 1", "Final"
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    ended_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(String(16), default="live", index=True)  # 'live'|'provisional'|'official'|'cancelled'


    event: Mapped[Event] = relationship(back_populates="sessions")
    race_class: Mapped[RaceClass] = relationship(back_populates="sessions")

    laps: Mapped[List["Lap"]] = relationship(back_populates="session", cascade="all, delete-orphan")
    results: Mapped[List["Result"]] = relationship(back_populates="session", cascade="all, delete-orphan")
    penalties: Mapped[List["Penalty"]] = relationship(back_populates="session", cascade="all, delete-orphan")

    __table_args__ = (
        # Avoid duplicate sessions if you synthesize unique_key absent a true UID
        UniqueConstraint("event_id", "class_id", "session_name", name="uq_session_event_class_name"),
    )


class Lap(Base):
    __tablename__ = "laps"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    session_id: Mapped[int] = mapped_column(ForeignKey("sessions.id", ondelete="CASCADE"), index=True)
    driver_id: Mapped[int] = mapped_column(ForeignKey("drivers.id", ondelete="CASCADE"), index=True)

    lap_number: Mapped[int] = mapped_column(Integer, nullable=False)
    lap_time_ms: Mapped[int] = mapped_column(Integer, nullable=False)  # Lap time in milliseconds
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    is_valid: Mapped[bool] = mapped_column(Integer, nullable=False, default=1)  # 1 for valid, 0 for invalid

    __table_args__ = (
        UniqueConstraint('session_id', 'driver_id', 'lap_number', name='uq_session_driver_lap'),
        Index('ix_session_driver_lap_time', 'session_id', 'driver_id', 'lap_time_ms'),
    )

     # Useful composite indexes
    __mapper_args__ = {}
    # For frequent lookups: (session, driver) and (session, lap_no)
Index("ix_laps_session_driver", Lap.session_id, Lap.driver_id)
Index("ix_laps_session_lapno", Lap.session_id, Lap.lap_no)

class Penalty(Base):
    __tablename__ = "penalties"

    id: Mapped[int] = mapped_column(primary_key=True)
    session_id: Mapped[int] = mapped_column(ForeignKey("sessions.id", ondelete="CASCADE"), index=True)
    driver_id: Mapped[int] = mapped_column(ForeignKey("drivers.id", ondelete="CASCADE"), index=True)
    type: Mapped[str] = mapped_column(PenaltyTypeEnum)
    value_ms: Mapped[Optional[int]] = mapped_column(Integer)             # TIME
    value_positions: Mapped[Optional[int]] = mapped_column(Integer)      # POSITION
    note: Mapped[Optional[str]] = mapped_column(Text)
    source: Mapped[Optional[str]] = mapped_column(String(64))            # 'official_sheet','race_director','tech'
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)

    session: Mapped[Session] = relationship(back_populates="penalties")
    driver: Mapped[Driver] = relationship()


class Result(Base):
    __tablename__ = "results"

    id: Mapped[int] = mapped_column(primary_key=True)
    session_id: Mapped[int] = mapped_column(ForeignKey("sessions.id", ondelete="CASCADE"), index=True)
    driver_id: Mapped[int] = mapped_column(ForeignKey("drivers.id", ondelete="CASCADE"), index=True)

    version: Mapped[int] = mapped_column(Integer)             # 1..N (increment when you freeze)
    basis: Mapped[str] = mapped_column(BasisEnum, index=True) # 'provisional' | 'official'

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
        # fast fetch for "latest version for basis"
        Index("ix_results_session_basis_version", "session_id", "basis", "version"),
        UniqueConstraint("session_id", "driver_id", "basis", "version", name="uq_result_versioned_row"),
        CheckConstraint("position IS NULL OR position > 0", name="ck_result_pos_pos"),
    )

class Point(Base):
    __tablename__ = "points"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(128), nullable=False)  # e.g., "Championship 2024"
    bonus_lap: Mapped[bool] = mapped_column(Integer, nullable=False, default=0)  # 1 if bonus lap points
    bonus_pole: Mapped[bool] = mapped_column(Integer, nullable=False, default=0) # 1 if bonus pole position points


    scales: Mapped[List["PointScale"]] = relationship(back_populates="point", cascade="all, delete-orphan")


class PointScale(Base):
    __tablename__ = "points_scales"

    id: Mapped[int] = mapped_column(primary_key=True)
    point_id: Mapped[int] = mapped_column(ForeignKey("point.id", ondelete="CASCADE"), index=True)
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
    basis: Mapped[str] = mapped_column(BasisEnum, index=True)    # should be 'official' for exports
    version: Mapped[int] = mapped_column(Integer)

    position: Mapped[Optional[int]] = mapped_column(Integer)
    base_points: Mapped[int] = mapped_column(Integer, default=0)
    bonus_points: Mapped[int] = mapped_column(Integer, default=0)
    total_points: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    driver: Mapped[Driver] = relationship(back_populates="points")
    session: Mapped[Session] = relationship()

    __table_args__ = (
        UniqueConstraint("session_id", "driver_id", "basis", "version", name="uq_points_versioned_row"),
    )
    


