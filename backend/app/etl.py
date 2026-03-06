"""ETL pipeline: fetch data from the autochecker API and load it into the database.

The autochecker dashboard API provides two endpoints:
- GET /api/items — lab/task catalog
- GET /api/logs  — anonymized check results (supports ?since= and ?limit= params)

Both require HTTP Basic Auth (email + password from settings).
"""

from datetime import datetime

import httpx
from sqlalchemy import func, select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.settings import settings


# ---------------------------------------------------------------------------
# Extract — fetch data from the autochecker API
# ---------------------------------------------------------------------------


async def fetch_items() -> list[dict]:
    """Fetch the lab/task catalog from the autochecker API.

    Returns:
        A JSON array of objects with keys:
        lab (str), task (str | null), title (str), type ("lab" | "task")
    """
    async with httpx.AsyncClient() as client:
        url = f"{settings.autochecker_api_url}/api/items"
        response = await client.get(
            url,
            auth=(settings.autochecker_email, settings.autochecker_password),
        )
        response.raise_for_status()
        return response.json()


async def fetch_logs(since: datetime | None = None) -> list[dict]:
    """Fetch check results from the autochecker API.

    Args:
        since: Optional timestamp to fetch logs since this point (incremental sync).

    Returns:
        List of all log dicts from all pages (handles pagination automatically).
    """
    all_logs: list[dict] = []
    current_since = since

    async with httpx.AsyncClient() as client:
        url = f"{settings.autochecker_api_url}/api/logs"

        while True:
            params = {"limit": 500}
            if current_since is not None:
                params["since"] = current_since.isoformat()

            response = await client.get(
                url,
                params=params,
                auth=(settings.autochecker_email, settings.autochecker_password),
            )
            response.raise_for_status()

            data = response.json()
            logs = data.get("logs", [])
            all_logs.extend(logs)

            # Check if there are more pages
            if not data.get("has_more", False) or not logs:
                break

            # Use the last log's submitted_at as the next "since" value
            last_log = logs[-1]
            current_since = datetime.fromisoformat(last_log["submitted_at"])

    return all_logs


# ---------------------------------------------------------------------------
# Load — insert fetched data into the local database
# ---------------------------------------------------------------------------


async def load_items(items: list[dict], session: AsyncSession) -> int:
    """Load items (labs and tasks) into the database.

    Args:
        items: Raw item dicts from fetch_items().
        session: Database session.

    Returns:
        Number of newly created items.
    """
    from app.models.item import ItemRecord

    new_count = 0
    lab_map: dict[str, ItemRecord] = {}

    # Process labs first
    labs = [item for item in items if item["type"] == "lab"]
    for lab_item in labs:
        lab_title = lab_item["title"]

        # Check if lab already exists
        stmt = select(ItemRecord).where(
            (ItemRecord.type == "lab") & (ItemRecord.title == lab_title)
        )
        result = await session.execute(stmt)
        existing = result.scalar_one_or_none()

        if existing is None:
            # Create new lab
            new_lab = ItemRecord(type="lab", title=lab_title)
            session.add(new_lab)
            await session.flush()  # Flush to get the ID
            new_count += 1
            lab_map[lab_item["lab"]] = new_lab
        else:
            lab_map[lab_item["lab"]] = existing

    # Process tasks
    tasks = [item for item in items if item["type"] == "task"]
    for task_item in tasks:
        task_title = task_item["title"]
        lab_short_id = task_item["lab"]

        # Get parent lab
        if lab_short_id not in lab_map:
            continue  # Skip if parent lab doesn't exist

        parent_lab = lab_map[lab_short_id]

        # Check if task already exists
        stmt = select(ItemRecord).where(
            (ItemRecord.type == "task")
            & (ItemRecord.title == task_title)
            & (ItemRecord.parent_id == parent_lab.id)
        )
        result = await session.execute(stmt)
        existing = result.scalar_one_or_none()

        if existing is None:
            # Create new task
            new_task = ItemRecord(
                type="task", title=task_title, parent_id=parent_lab.id
            )
            session.add(new_task)
            new_count += 1

    # Commit all changes
    await session.commit()

    return new_count


async def load_logs(
    logs: list[dict], items_catalog: list[dict], session: AsyncSession
) -> int:
    """Load interaction logs into the database.

    Args:
        logs: Raw log dicts from the API (each has lab, task, student_id, etc.)
        items_catalog: Raw item dicts from fetch_items() — needed to map
            short IDs (e.g. "lab-01", "setup") to item titles stored in the DB.
        session: Database session.

    Returns:
        Number of newly created interactions.
    """
    from app.models.interaction import InteractionLog
    from app.models.item import ItemRecord
    from app.models.learner import Learner

    new_count = 0

    # Build lookup from (lab, task) to title
    title_lookup: dict[tuple[str, str | None], str] = {}
    for item in items_catalog:
        if item["type"] == "lab":
            title_lookup[(item["lab"], None)] = item["title"]
        elif item["type"] == "task":
            title_lookup[(item["lab"], item["task"])] = item["title"]

    # Process each log
    for log in logs:
        lab_short_id = log["lab"]
        task_short_id = log.get("task")

        # Find the item title
        item_title = title_lookup.get((lab_short_id, task_short_id))
        if item_title is None:
            continue  # Skip if we can't find the item

        # Find or create learner
        learner_id = log["student_id"]
        stmt = select(Learner).where(Learner.external_id == learner_id)
        result = await session.execute(stmt)
        learner = result.scalar_one_or_none()

        if learner is None:
            learner = Learner(
                external_id=learner_id, student_group=log.get("group", "")
            )
            session.add(learner)
            await session.flush()

        # Find the item record
        # Distinguish labs from tasks: use parent_id to determine type
        if task_short_id is None:
            # This is a lab (no task)
            stmt = select(ItemRecord).where(
                (ItemRecord.type == "lab") & (ItemRecord.title == item_title)
            )
        else:
            # This is a task: find parent lab first, then find task by parent_id
            lab_title = title_lookup.get((lab_short_id, None))
            if lab_title is None:
                continue  # Parent lab not found
            
            stmt = select(ItemRecord).where(
                (ItemRecord.type == "lab") & (ItemRecord.title == lab_title)
            )
            result = await session.execute(stmt)
            parent_lab = result.scalar_one_or_none()
            
            if parent_lab is None:
                continue  # Parent lab not found
            
            # Now find the task
            stmt = select(ItemRecord).where(
                (ItemRecord.type == "task")
                & (ItemRecord.title == item_title)
                & (ItemRecord.parent_id == parent_lab.id)
            )
        
        result = await session.execute(stmt)
        item = result.scalar_one_or_none()

        if item is None:
            continue  # Skip if item not found

        # Check if this log already exists
        stmt = select(InteractionLog).where(InteractionLog.external_id == log["id"])
        result = await session.execute(stmt)
        if result.scalar_one_or_none() is not None:
            continue  # Skip, already exists

        # Create interaction log
        interaction = InteractionLog(
            external_id=log["id"],
            learner_id=learner.id,
            item_id=item.id,
            kind="attempt",
            score=log.get("score"),
            checks_passed=log.get("passed"),
            checks_total=log.get("total"),
            created_at=datetime.fromisoformat(log["submitted_at"]),
        )
        session.add(interaction)
        new_count += 1

    # Commit all changes
    await session.commit()

    return new_count


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


async def sync(session: AsyncSession) -> dict:
    """Run the full ETL pipeline.

    Returns:
        Dict with:
        - new_records: number of newly created interactions
        - total_records: total interactions in the database
    """
    from app.models.interaction import InteractionLog

    # Step 1: Fetch and load items
    items_catalog = await fetch_items()
    await load_items(items_catalog, session)

    # Step 2: Find the last synced timestamp
    stmt = select(func.max(InteractionLog.created_at))
    result = await session.execute(stmt)
    last_sync = result.scalar()

    # Step 3: Fetch and load logs since last sync
    logs = await fetch_logs(since=last_sync)
    new_records = await load_logs(logs, items_catalog, session)

    # Get total records
    stmt = select(func.count(InteractionLog.id))
    result = await session.execute(stmt)
    total_records = result.scalar() or 0

    return {
        "new_records": new_records,
        "total_records": total_records,
    }
