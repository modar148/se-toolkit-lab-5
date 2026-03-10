from fastapi import APIRouter, Depends, Query
from sqlalchemy import select, func, case
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlalchemy import select, func, case, cast, Numeric

from app.database import get_session
from app.models.interaction import InteractionLog
from app.models.item import ItemRecord
from app.models.learner import Learner

router = APIRouter()


def get_task_subquery(lab: str):
    """Helper to find all task IDs belonging to a specific lab."""
    # Transforms "lab-04" -> "Lab 04"
    lab_title = lab.replace("-", " ").title()
    
    return select(ItemRecord.id).where(
        ItemRecord.parent_id.in_(
            select(ItemRecord.id).where(
                ItemRecord.title.ilike(f"%{lab_title}%"),
                ItemRecord.type == "lab"
            )
        )
    )


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    tasks_stmt = get_task_subquery(lab)

    # Use CASE WHEN to bucket the scores into 4 groups
    stmt = select(
        func.sum(case((InteractionLog.score <= 25, 1), else_=0)).label("bucket_1"),
        func.sum(case(((InteractionLog.score > 25) & (InteractionLog.score <= 50), 1), else_=0)).label("bucket_2"),
        func.sum(case(((InteractionLog.score > 50) & (InteractionLog.score <= 75), 1), else_=0)).label("bucket_3"),
        func.sum(case((InteractionLog.score > 75, 1), else_=0)).label("bucket_4"),
    ).where(
        InteractionLog.item_id.in_(tasks_stmt),
        InteractionLog.score.isnot(None)
    )

    # Changed execute to exec
    result = (await session.exec(stmt)).first()

    # Always return all four buckets, even if count is 0
    return [
        {"bucket": "0-25", "count": int(result.bucket_1 or 0)},
        {"bucket": "26-50", "count": int(result.bucket_2 or 0)},
        {"bucket": "51-75", "count": int(result.bucket_3 or 0)},
        {"bucket": "76-100", "count": int(result.bucket_4 or 0)},
    ]


@router.get("/pass-rates")
async def get_pass_rates(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    tasks_stmt = get_task_subquery(lab)

    stmt = (
        select(
            ItemRecord.title.label("task"),
            # CAST the avg result to Numeric before rounding
            func.round(cast(func.avg(InteractionLog.score), Numeric), 1).label("avg_score"),
            func.count(InteractionLog.id).label("attempts")
        )
        .join(InteractionLog, ItemRecord.id == InteractionLog.item_id)
        .where(ItemRecord.id.in_(tasks_stmt))
        .group_by(ItemRecord.title)
        .order_by(ItemRecord.title)
    )

    results = await session.exec(stmt)

    return [
        {"task": row.task, "avg_score": float(row.avg_score or 0), "attempts": row.attempts}
        for row in results
    ]


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    tasks_stmt = get_task_subquery(lab)

    # Use func.date() to group by day as requested in the stub
    stmt = (
        select(
            func.date(InteractionLog.created_at).label("date"),
            func.count(InteractionLog.id).label("submissions")
        )
        .where(InteractionLog.item_id.in_(tasks_stmt))
        .group_by(func.date(InteractionLog.created_at))
        .order_by(func.date(InteractionLog.created_at))
    )

    # Changed execute to exec
    results = await session.exec(stmt)

    return [
        {"date": str(row.date), "submissions": row.submissions}
        for row in results
    ]


@router.get("/groups")
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    tasks_stmt = get_task_subquery(lab)

    stmt = (
        select(
            Learner.student_group.label("group"),
            # CAST the avg result to Numeric before rounding
            func.round(cast(func.avg(InteractionLog.score), Numeric), 1).label("avg_score"),
            func.count(func.distinct(InteractionLog.learner_id)).label("students")
        )
        .join(InteractionLog, Learner.id == InteractionLog.learner_id)
        .where(InteractionLog.item_id.in_(tasks_stmt))
        .group_by(Learner.student_group)
        .order_by(Learner.student_group)
    )

    results = await session.exec(stmt)

    return [
        {"group": row.group, "avg_score": float(row.avg_score or 0), "students": row.students}
        for row in results
    ]