from concurrent.futures import ThreadPoolExecutor
import datetime
import typing as t
from mock import patch, MagicMock
import uuid
import wsgiref.simple_server

import aiohttp
from apd.sensors.base import Sensor, HistoricalSensor, JSONSensor
from apd.sensors.exceptions import DataCollectionError
from apd.sensors.sensors import PythonVersion, ACStatus
from apd.sensors.wsgi import set_up_config
from apd.sensors.wsgi import v31
import flask
import pytest
from sqlalchemy.sql import Insert

from apd.aggregation import collect
from apd.aggregation.database import Deployment
from .utils import FakeHistoricalSensor, run_server_in_thread

pytestmark = [pytest.mark.functional]


@pytest.fixture
def sensors() -> t.Iterator[t.List[Sensor[t.Any]]]:
    """ Patch the get_sensors method to return a known pair of sensors only """
    data: t.List[Sensor[t.Any]] = [PythonVersion(), ACStatus()]
    with patch("apd.sensors.cli.get_sensors") as get_sensors:
        get_sensors.return_value = data
        yield data


@pytest.fixture(scope="module")
def bad_api_key_http_server():
    yield from run_server_in_thread(
        "alternate",
        {
            "APD_SENSORS_API_KEY": "penny",
            "APD_SENSORS_DEPLOYMENT_ID": "38cf2bae9adb445fad946c82e290487a",
        },
        12082,
    )


class TestGetDataPoints:
    @pytest.fixture
    def mut(self):
        return collect.get_data_points

    @pytest.mark.asyncio
    async def test_get_data_points(
        self, sensors: t.List[Sensor[t.Any]], mut, http_server: str
    ) -> None:
        # Get the data from the server, storing the time before and after
        # as bounds for the collected_at value
        async with aiohttp.ClientSession() as http:
            collect.http_session_var.set(http)
            time_before = datetime.datetime.now()
            results = await mut(http_server, "testing")
            time_after = datetime.datetime.now()

        assert len(results) == len(sensors) == 2

        for (sensor, result) in zip(sensors, results):
            try:
                assert sensor.from_json_compatible(result.data) == sensor.value()
            except DataCollectionError:
                continue
            assert result.sensor_name == sensor.name
            assert time_before <= result.collected_at <= time_after

    @pytest.mark.asyncio
    async def test_get_data_points_fails_with_bad_api_key(
        self, sensors: t.List[Sensor[t.Any]], mut, http_server: str
    ) -> None:
        with pytest.raises(
            ValueError,
            match=f"Error loading data from {http_server}: Supply API key in X-API-Key header",
        ):
            async with aiohttp.ClientSession() as http:
                collect.http_session_var.set(http)
                await mut(http_server, "incorrect")


class TestGetKnownSensors:
    @pytest.fixture
    def mut(self):
        return collect.get_known_sensors

    @pytest.mark.asyncio
    async def test_get_known_sensors(
        self, sensors: t.List[Sensor[t.Any]], mut, http_server: str
    ) -> None:
        async with aiohttp.ClientSession() as http:
            collect.http_session_var.set(http)
            results = await mut(
                http_server,
                "testing",
            )
        assert len(results) == 2
        print(results)


class TestGetHistoricalDataPoints:
    @pytest.fixture
    def sensors(self) -> t.Iterator[t.List[Sensor[t.Any]]]:
        """ Patch the get_sensors method to return a simple fake historical sensor """

        data: t.List[Sensor[t.Any]] = [FakeHistoricalSensor()]
        with patch("apd.sensors.cli.get_sensors") as get_sensors:
            get_sensors.return_value = data
            yield data

    @pytest.fixture
    def mut(self):
        return collect.get_historical_data_points_since

    @pytest.mark.asyncio
    async def test_get_historical_data_points_since(
        self, sensors: t.List[Sensor[t.Any]], mut, http_server: str
    ) -> None:
        async with aiohttp.ClientSession() as http:
            collect.http_session_var.set(http)
            results = await mut(
                http_server,
                "testing",
                datetime.datetime.now() - datetime.timedelta(hours=5),
                "fake",
            )
        assert len(results) == 5

    @pytest.mark.asyncio
    async def test_get_historical_data_points_since_fails_with_bad_api_key(
        self, sensors: t.List[Sensor[t.Any]], mut, http_server: str
    ) -> None:
        with pytest.raises(
            ValueError,
            match=f"Error loading data from {http_server}: Supply API key in X-API-Key header",
        ):
            async with aiohttp.ClientSession() as http:
                collect.http_session_var.set(http)
                await mut(
                    http_server,
                    "incorrect",
                    datetime.datetime.now() - datetime.timedelta(hours=1),
                    "fake",
                )


class TestCollectBothSensorTypes:
    @pytest.fixture
    def sensors(self) -> t.Iterator[t.List[Sensor[t.Any]]]:
        """ Patch the get_sensors method to return a simple fake historical sensor """
        data: t.List[Sensor[t.Any]] = [FakeHistoricalSensor(), PythonVersion()]
        with patch("apd.sensors.cli.get_sensors") as get_sensors:
            get_sensors.return_value = data
            yield data

    @pytest.fixture
    def mut(self):
        return collect.add_data_from_sensors

    @pytest.fixture
    def mock_db_session(self):
        return MagicMock()

    @pytest.mark.asyncio
    async def test_collect_data(
        self, sensors: t.List[Sensor[t.Any]], mut, http_server: str, mock_db_session
    ) -> None:
        async with aiohttp.ClientSession() as http:
            collect.http_session_var.set(http)
            with patch(
                "apd.aggregation.collect.most_recent_data_point_for_sensor_and_server"
            ) as most_recent:
                most_recent.return_value = datetime.datetime.now() - datetime.timedelta(
                    hours=40
                )
                results = await mut(
                    mock_db_session,
                    [
                        Deployment(
                            id=uuid.uuid4(),
                            uri=http_server,
                            name="Test Server",
                            colour=None,
                            api_key="testing",
                        ),
                    ],
                )

        # We expect 1 current fake result, 1 current python version, and n historical, where n is 40 here
        assert len(results) == 42


class TestAddDataFromSensors:
    @pytest.fixture
    def mut(self):
        return collect.add_data_from_sensors

    @pytest.fixture
    def mock_db_session(self):
        return MagicMock()

    @pytest.mark.asyncio
    async def test_get_get_data_from_sensors(
        self, mock_db_session, sensors: t.List[Sensor[t.Any]], mut, http_server: str
    ) -> None:
        results = await mut(
            mock_db_session,
            [
                Deployment(
                    id=None, colour=None, name=None, uri=http_server, api_key="testing"
                )
            ],
        )
        assert mock_db_session.execute.call_count == len(sensors) * 2
        assert len(results) == len(sensors)

    @pytest.mark.asyncio
    async def test_get_get_data_from_sensors_with_multiple_servers(
        self, mock_db_session, sensors: t.List[Sensor[t.Any]], mut, http_server: str
    ) -> None:
        results = await mut(
            mock_db_session,
            [
                Deployment(
                    id=None, colour=None, name=None, uri=http_server, api_key="testing"
                ),
                Deployment(
                    id=None, colour=None, name=None, uri=http_server, api_key="testing"
                ),
            ],
        )
        assert mock_db_session.execute.call_count == len(sensors) * 4
        assert len(results) == len(sensors) * 2

    @pytest.mark.asyncio
    async def test_data_points_added_if_only_partial_success(
        self,
        mock_db_session,
        sensors: t.List[Sensor[t.Any]],
        mut,
        http_server: str,
        bad_api_key_http_server: str,
        caplog,
    ) -> None:
        await mut(
            mock_db_session,
            [
                Deployment(
                    id=None,
                    colour=None,
                    name=None,
                    uri=http_server,
                    api_key="testing",
                ),
                Deployment(
                    id=None,
                    colour=None,
                    name=None,
                    uri=bad_api_key_http_server,
                    api_key="testing",
                ),
            ],
        )
        # We expect Python Version and AC status for one endpoint
        assert mock_db_session.execute.call_count == 4
        insertion_calls = mock_db_session.execute.call_args_list

        params = [
            call[0][0]._values
            for call in insertion_calls
            if isinstance(call[0][0], Insert)
        ]
        assert {insertion["sensor_name"].value for insertion in params} == {
            "PythonVersion",
            "ACStatus",
        }
        assert {insertion["deployment_id"].value for insertion in params} == {
            uuid.UUID("a46b1d1207fd4cdcad39bbdf706dfe29"),
        }

        # We should also have a log message showing details of the failing server and the failure
        assert len(caplog.records) == 1
        assert caplog.records[0].message == "Data retrieval failed"
        assert bad_api_key_http_server in caplog.records[0].exc_text
        assert "Supply API key in X-API-Key header" in caplog.records[0].exc_text
