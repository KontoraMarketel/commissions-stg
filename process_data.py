import json
import pandas as pd
from uuid import UUID, uuid5, NAMESPACE_OID
from datetime import datetime, timezone
from clickhouse_connect.driver.asyncclient import AsyncClient


async def process_data(db_conn: AsyncClient, data: dict, task_id: str, ts: str) -> None:
    # task_id -> UUID
    try:
        task_uuid = UUID(str(task_id))
    except Exception:
        task_uuid = uuid5(NAMESPACE_OID, str(task_id))

    # ts -> datetime (UTC)
    if isinstance(ts, (int, float)):
        ts_dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    else:
        ts_dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))

    # data["data"] приходит как строка
    raw = data.get("data")
    if not raw:
        return
    if isinstance(raw, str):
        items = json.loads(raw)
    else:
        items = raw

    if not items:
        return

    # конвертируем список словарей в DataFrame
    df = pd.DataFrame(items)

    # маппинг колонок camelCase -> snake_case под твою таблицу
    df = df.rename(columns={
        "kgvpBooking": "kgvp_booking",
        "kgvpMarketplace": "kgvp_marketplace",
        "kgvpPickup": "kgvp_pickup",
        "kgvpSupplier": "kgvp_supplier",
        "kgvpSupplierExpress": "kgvp_supplier_express",
        "paidStorageKgvp": "paid_storage_kgvp",
        "parentID": "parent_id",
        "parentName": "parent_name",
        "subjectID": "subject_id",
        "subjectName": "subject_name",
    })

    # добавим task_id и ts во все строки
    df["task_id"] = str(task_uuid)
    df["ts"] = ts_dt

    # порядок колонок под таблицу
    df = df[
        [
            "kgvp_booking", "kgvp_marketplace", "kgvp_pickup",
            "kgvp_supplier", "kgvp_supplier_express", "paid_storage_kgvp",
            "parent_id", "parent_name",
            "subject_id", "subject_name",
            "task_id", "ts"
        ]
    ]

    # вставляем DataFrame в ClickHouse
    await db_conn.insert_df("stg_commissions", df,
                            settings={"async_insert": 1, "wait_for_async_insert": 0})
