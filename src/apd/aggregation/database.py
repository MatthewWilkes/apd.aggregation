from __future__ import annotations

from dataclasses import dataclass, field, asdict
import datetime
import typing as t

import sqlalchemy
import sqlalchemy.util._collections
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import Table


metadata = sqlalchemy.MetaData()

datapoint_table = Table(
    "sensor_values",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("sensor_name", sqlalchemy.String),
    sqlalchemy.Column("collected_at", TIMESTAMP),
    sqlalchemy.Column("data", JSONB),
)


@dataclass
class DataPoint:
    sensor_name: str
    data: t.Dict[str, t.Any]
    id: t.Optional[int] = None
    collected_at: datetime.datetime = field(default_factory=datetime.datetime.now)

    @classmethod
    def from_sql_result(cls, result) -> DataPoint:
        return cls(**result._asdict())

    def _asdict(self) -> t.Dict[str, t.Any]:
        data = asdict(self)
        if data["id"] is None:
            del data["id"]
        return data


def main():
    engine = sqlalchemy.create_engine(
        "postgresql+psycopg2://apd@localhost/apd", echo=True
    )
    sm = sessionmaker(engine)
    Session = sm()
    if False:
        metadata.create_all(engine)
    print(Session.query(DataPoint).all())
    pass


if __name__ == "__main__":
    main()
