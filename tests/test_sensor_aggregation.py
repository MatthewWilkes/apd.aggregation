from __future__ import annotations

import contextlib
from dataclasses import dataclass
import json
import typing as t
from unittest.mock import patch, Mock

import pytest

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
    return FakeAIOHttpClient({"http://localhost/v/2.0/sensors/": json.dumps(data)})


class TestGetDataPoints:
    @pytest.fixture
    def mut(self):
        return apd.aggregation.collect.get_data_points

    @pytest.mark.asyncio
    async def test_get_data_points(
        self, mut, mockclient: FakeAIOHttpClient, data
    ) -> None:
        datapoints = await mut("http://localhost", "", mockclient)
        # datapoints = await mut("http://localhost", "", client)

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
        return Mock()

    @pytest.mark.asyncio
    async def test_datapoints_are_added_to_the_session(self, mut, db_session) -> None:
        assert db_session.add.call_count == 0
        datapoints = await mut(db_session, ["http://localhost"], "")
        assert db_session.add.call_count == len(datapoints)
