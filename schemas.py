from pydantic import BaseModel
from datetime import datetime
from typing import List, Optional

class WorkflowBase(BaseModel):
    workflow_id: str
    status: str
    duration_seconds: Optional[float] = None
    error_count: int = 0
    records_processed: int = 0
    department: Optional[str] = None
    priority: str = "medium"
    notes: Optional[str] = None

class WorkflowCreate(WorkflowBase):
    pass

class WorkflowResponse(WorkflowBase):
    id: int
    timestamp: datetime
    data_quality_score: float

    class Config:
        from_attributes = True

class KPIResponse(BaseModel):
    metric_name: str
    value: float
    timestamp: datetime
    category: str
    description: Optional[str] = None

class ExceptionCreate(BaseModel):
    workflow_id: str
    exception_type: str
    severity: str = "medium"
    description: str

class ExceptionResponse(ExceptionCreate):
    id: int
    timestamp: datetime
    resolved: bool = False

class MetricsResponse(BaseModel):
    total_workflows: int
    avg_duration: float
    error_rate: float
    data_quality_avg: float
    high_priority_exceptions: int
    kpis: List[KPIResponse]
