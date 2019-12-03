from __future__ import annotations

import contextlib
from dataclasses import dataclass
import json
import typing as t
from unittest.mock import patch, Mock

from alembic.config import Config
from alembic.script import ScriptDirectory
from alembic.runtime.environment import EnvironmentContext
import pytest
import sqlalchemy

import apd.aggregation.collect


@pytest.fixture
def data() -> t.Any:
    return {
        "sensors": [
            {
                "human_readable": "3.7",
                "id": "PythonVersion",
                "title": "Python Version",
                "value": [3, 7, 2, "final", 0],
            },
            {
                "human_readable": "Not connected",
                "id": "ACStatus",
                "title": "AC Connected",
                "value": False,
            },
        ]
    }


@dataclass
class FakeAIOHttpClient:
    responses: t.Dict[str, str]

    @contextlib.asynccontextmanager
    async def get(
        self, url: str, headers: t.Optional[t.Dict[str, str]] = None
    ) -> t.AsyncIterator[FakeAIOHttpResponse]:
        if url in self.responses:
            yield FakeAIOHttpResponse(body=self.responses[url])
        else:
            yield FakeAIOHttpResponse(body="", status=404)


@dataclass
class FakeAIOHttpResponse:
    body: str
    status: int = 200

    async def json(self) -> t.Any:
        return json.loads(self.body)


@pytest.fixture
def mockclient(data) -> FakeAIOHttpClient:
    return FakeAIOHttpClient(
        {
            "http://localhost/v/2.1/sensors/": json.dumps(data),
            "http://localhost/v/2.1/deployment_id": json.dumps(
                {"deployment_id": "b29ba0ee10f14552b6b21327bb96d3fb"}
            ),
        }
    )


class TestGetDataPoints:
    @pytest.fixture
    def mut(self):
        return apd.aggregation.collect.get_data_points

    @pytest.mark.asyncio
    async def test_get_data_points(
        self, mut, mockclient: FakeAIOHttpClient, data
    ) -> None:
        token = apd.aggregation.collect.http_session_var.set(mockclient)
        try:
            datapoints = await mut("http://localhost", "")
        finally:
            apd.aggregation.collect.http_session_var.reset(token)

        assert len(datapoints) == len(data["sensors"])
        for sensor in data["sensors"]:
            assert sensor["value"] in (datapoint.data for datapoint in datapoints)
            assert sensor["id"] in (datapoint.sensor_name for datapoint in datapoints)


class TestAddDataFromSensors:
    @pytest.fixture
    def mut(self):
        return apd.aggregation.collect.add_data_from_sensors

    @pytest.fixture(autouse=True)
    def patch_aiohttp(self, mockclient):
        with patch("aiohttp.ClientSession") as ClientSession:
            ClientSession.return_value.__aenter__.return_value = mockclient
            yield ClientSession

    @pytest.fixture
    def db_session(self):
        session = Mock()
        sql_result = session.execute.return_value
        sql_result.inserted_primary_key = [1]
        return session

    @pytest.mark.asyncio
    async def test_datapoints_are_added_to_the_session(self, mut, db_session) -> None:
        assert db_session.execute.call_count == 0
        datapoints = await mut(db_session, ["http://localhost"], "")
        assert db_session.execute.call_count == len(datapoints)


class TestDatabaseConnection:
    @pytest.fixture
    def db_uri(self):
        return "postgresql+psycopg2://apd@localhost/apd-test"

    @pytest.fixture
    def migrated_db(self, db_uri):
        config = Config()
        config.set_main_option("script_location", "apd.aggregation:alembic")
        config.set_main_option("sqlalchemy.url", db_uri)
        script = ScriptDirectory.from_config(config)

        def upgrade(rev, context):
            return script._upgrade_revs(script.get_current_head(), rev)

        def downgrade(rev, context):
            return script._downgrade_revs(None, rev)

        with EnvironmentContext(config, script, fn=upgrade):
            script.run_env()

        yield

        with EnvironmentContext(config, script, fn=downgrade):
            script.run_env()

    @pytest.fixture
    def db_session(self, migrated_db, db_uri):
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker

        engine = create_engine(db_uri, echo=True)
        sm = sessionmaker(engine)
        Session = sm()
        yield Session
        Session.close()

    @pytest.fixture(autouse=True)
    def patch_aiohttp(self, mockclient):
        with patch("aiohttp.ClientSession") as ClientSession:
            ClientSession.return_value.__aenter__.return_value = mockclient
            yield ClientSession

    @pytest.fixture
    def table(self):
        return apd.aggregation.database.datapoint_table

    @pytest.fixture
    def daily_summary_view(self):
        return apd.aggregation.database.daily_summary_view

    @pytest.fixture
    def model(self):
        return apd.aggregation.database.DataPoint

    @pytest.fixture
    def mut(self):
        return apd.aggregation.collect.add_data_from_sensors

    @pytest.mark.asyncio
    async def test_datapoints_are_added_to_the_session(self, db_session, table) -> None:
        datapoints = await apd.aggregation.collect.add_data_from_sensors(
            db_session, ["http://localhost"], ""
        )
        num_points = db_session.query(table).count()
        assert num_points == len(datapoints) == 2

    @pytest.mark.asyncio
    async def test_datapoints_can_be_mapped_back_to_DataPoints(
        self, mut, db_session, table, model
    ) -> None:
        datapoints = await mut(db_session, ["http://localhost"], "")
        db_points = [
            model.from_sql_result(result) for result in db_session.query(table)
        ]
        assert db_points == datapoints

    @pytest.mark.asyncio
    async def test_daily_summary_view_matches_query(
        self, db_session, model, table, daily_summary_view
    ) -> None:
        await apd.aggregation.collect.add_data_from_sensors(
            db_session, ["http://localhost"], ""
        )

        headers = table.c.sensor_name, table.c.data
        value_counts = (
            db_session.query(*headers, sqlalchemy.func.count(table.c.id))
            .filter(model.collected_on_date == sqlalchemy.func.current_date())
            .group_by(*headers)
        )

        daily_summary_view = db_session.query(daily_summary_view)
        assert value_counts.all() == daily_summary_view.all()
