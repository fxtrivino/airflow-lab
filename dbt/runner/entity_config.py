import re

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class EntityConfig:
    name: str
    table: str
    stage: str
    data_dir: Path
    filename_pattern: re.Pattern


ENTITY_CONFIG = {
    "customers": EntityConfig(
        name="customers",
        table="CUSTOMERS_RAW",
        stage="CUSTOMERS_STAGE",
        data_dir=Path("/opt/dbt/data/customers"),
        filename_pattern=re.compile(
            r"customers_batch_[0-9]+\.csv"
        ),
    ),
    "orders": EntityConfig(
        name="orders",
        table="ORDERS_RAW",
        stage="ORDERS_STAGE",
        data_dir=Path("/opt/dbt/data/orders"),
        filename_pattern=re.compile(
            r"orders_batch_[0-9]+\.csv"
        ),
    ),
    "payments": EntityConfig(
        name="payments",
        table="PAYMENTS_RAW",
        stage="PAYMENTS_STAGE",
        data_dir=Path("/opt/dbt/data/payments"),
        filename_pattern=re.compile(
            r"payments_batch_[0-9]+\.csv"
        ),
    ),
}


def get_entity_config(entity: str) -> EntityConfig:
    try:
        return ENTITY_CONFIG[entity.lower()]
    except KeyError:
        raise ValueError(
            f"Unsupported entity: {entity}"
        ) from None