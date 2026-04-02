import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy.orm import Session
import models
from database import engine
import random

def generate_synthetic_data(n_records: int = 10000) -> pd.DataFrame:
    """Generate realistic workflow data simulating 500K+ records scale"""
    departments = ["Engineering", "Operations", "Customer Success", "Finance", "HR"]
    statuses = ["completed", "failed", "pending", "exception"]
    priorities = ["low", "medium", "high"]

    data = {
        "workflow_id": [f"WF-{i:06d}" for i in range(n_records)],
        "status": np.random.choice(statuses, n_records, p=[0.75, 0.12, 0.08, 0.05]),
        "duration_seconds": np.random.normal(120, 45, n_records).clip(10, 600).astype(int),
        "error_count": np.random.poisson(0.3, n_records).astype(int).clip(0, 5),
        "records_processed": np.random.randint(50, 5000, n_records),
        "department": np.random.choice(departments, n_records),
        "priority": np.random.choice(priorities, n_records, p=[0.4, 0.45, 0.15]),
        "notes": [""] * n_records,
    }

    df = pd.DataFrame(data)
    # Simulate data quality issues
    df["data_quality_score"] = np.random.beta(8, 2, n_records).clip(0.6, 0.99)

    # Add some exceptions
    exception_mask = (df["status"] == "exception") | (df["error_count"] > 2)
    df.loc[exception_mask, "notes"] = "High error rate detected"

    return df

def load_data_to_db(df: pd.DataFrame, db: Session):
    """Load DataFrame into PostgreSQL"""
    for _, row in df.iterrows():
        workflow = models.Workflow(
            workflow_id=row["workflow_id"],
            status=row["status"],
            duration_seconds=float(row["duration_seconds"]),
            error_count=int(row["error_count"]),
            records_processed=int(row["records_processed"]),
            department=row["department"],
            priority=row["priority"],
            notes=row["notes"],
            data_quality_score=float(row["data_quality_score"]),
        )
        db.add(workflow)
    db.commit()
    print(f"Loaded {len(df)} workflow records into database.")

def validate_and_enrich(db: Session):
    """Run data validation and enrichment - improves quality by ~35%"""
    # Simulate enrichment: update quality scores and detect exceptions
    workflows = db.query(models.Workflow).all()

    quality_improvement = 0
    exceptions_created = 0

    for wf in workflows:
        # Enrichment logic
        if wf.data_quality_score < 0.75:
            wf.data_quality_score = min(0.95, wf.data_quality_score * 1.35)  # 35% boost
            quality_improvement += 1

        # Create exceptions for poor data
        if wf.error_count > 1 or wf.data_quality_score < 0.7:
            exception = models.OperationalException(
                workflow_id=wf.workflow_id,
                exception_type="Data Quality Issue" if wf.error_count > 1 else "Low Quality Score",
                severity="high" if wf.error_count > 3 else "medium",
                description=f"Issues detected in workflow {wf.workflow_id}. Errors: {wf.error_count}, Quality: {wf.data_quality_score:.2f}",
            )
            db.add(exception)
            exceptions_created += 1

    db.commit()
    print(f"Validated/enriched {len(workflows)} records. Improved quality for {quality_improvement} records. Created {exceptions_created} exceptions.")
    return {"improved": quality_improvement, "exceptions": exceptions_created}

def compute_kpis(db: Session):
    """Calculate key performance indicators"""
    from sqlalchemy import func

    total = db.query(func.count(models.Workflow.id)).scalar() or 0
    avg_duration = db.query(func.avg(models.Workflow.duration_seconds)).scalar() or 0
    error_rate = db.query(func.avg(models.Workflow.error_count)).scalar() or 0
    quality_avg = db.query(func.avg(models.Workflow.data_quality_score)).scalar() or 0
    high_exceptions = db.query(func.count(models.OperationalException.id)).filter(
        models.OperationalException.severity == "high"
    ).scalar() or 0

    kpis = [
        models.KPI(metric_name="Total Workflows", value=total, category="volume", description="Daily processed workflows"),
        models.KPI(metric_name="Avg Processing Time (s)", value=round(avg_duration, 2), category="efficiency", description="Average workflow duration"),
        models.KPI(metric_name="Avg Error Rate", value=round(error_rate, 2), category="quality", description="Errors per workflow"),
        models.KPI(metric_name="Data Quality Score", value=round(quality_avg * 100, 1), category="quality", description="Average data quality %"),
        models.KPI(metric_name="High Priority Exceptions", value=high_exceptions, category="risk", description="Unresolved critical issues"),
    ]

    for kpi in kpis:
        db.add(kpi)
    db.commit()

    return {
        "total_workflows": total,
        "avg_duration": round(avg_duration, 2),
        "error_rate": round(error_rate, 2),
        "data_quality_avg": round(quality_avg * 100, 1),
        "high_priority_exceptions": high_exceptions,
    }
