from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, Text
from sqlalchemy.sql import func
from database import Base
from datetime import datetime

class Workflow(Base):
    __tablename__ = "workflows"

    id = Column(Integer, primary_key=True, index=True)
    workflow_id = Column(String, unique=True, index=True, nullable=False)
    status = Column(String, nullable=False)  # pending, completed, failed, exception
    timestamp = Column(DateTime, server_default=func.now())
    duration_seconds = Column(Float)
    error_count = Column(Integer, default=0)
    records_processed = Column(Integer, default=0)
    department = Column(String)
    priority = Column(String, default="medium")  # low, medium, high
    notes = Column(Text, nullable=True)
    data_quality_score = Column(Float, default=0.85)  # 0-1 scale
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

class KPI(Base):
    __tablename__ = "kpis"

    id = Column(Integer, primary_key=True, index=True)
    metric_name = Column(String, nullable=False)
    value = Column(Float, nullable=False)
    timestamp = Column(DateTime, server_default=func.now())
    category = Column(String)  # throughput, quality, efficiency
    description = Column(Text)

class OperationalException(Base):
    __tablename__ = "operational_exceptions"

    id = Column(Integer, primary_key=True, index=True)
    workflow_id = Column(String, nullable=False)
    exception_type = Column(String, nullable=False)
    severity = Column(String, default="medium")
    description = Column(Text)
    timestamp = Column(DateTime, server_default=func.now())
    resolved = Column(Boolean, default=False)
    resolution_notes = Column(Text, nullable=True)
