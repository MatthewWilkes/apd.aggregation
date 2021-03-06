from concurrent.futures import ThreadPoolExecutor
import datetime
import typing as t
from mock import patch, MagicMock
import uuid
import wsgiref.simple_server

import aiohttp
from apd.sensors.base import Sensor
from apd.sensors.exceptions import DataCollectionError
from apd.sensors.sensors import PythonVersion, ACStatus
from apd.sensors.wsgi import set_up_config
import flask
import pytest

from apd.sensors.wsgi import v21

from apd.aggregation import collect
from apd.aggregation.database import Deployment

pytestmark = [pytest.mark.functional]


@pytest.fixture
def sensors() -> t.Iterator[t.List[Sensor[t.Any]]]:
    """Patch the get_sensors method to return a known pair of sensors only"""
    data: t.List[Sensor[t.Any]] = [PythonVersion(), ACStatus()]
    with patch("apd.sensors.cli.get_sensors") as get_sensors:
        get_sensors.return_value = data
        yield data


def get_independent_flask_app(name: str) -> flask.Flask:
    """Create a new flask app with the v20 API blueprint loaded, so multiple copies
    of the app can be run in parallel without conflicting configuration"""
    app = flask.Flask(name)
    app.register_blueprint(v21.version, url_prefix="/v/2.1")
    return app


def run_server_in_thread(
    name: str, config: t.Dict[str, t.Any], port: int
) -> t.Iterator[str]:
    # Create a new flask app and load in required code, to prevent config conflicts
    app = get_independent_flask_app(name)
    flask_app = set_up_config(config, app)
    server = wsgiref.simple_server.make_server("localhost", port, flask_app)

    with ThreadPoolExecutor() as pool:
        pool.submit(server.serve_forever)
        yield f"http://localhost:{port}/"
        server.shutdown()


@pytest.fixture(scope="module")
def http_server():
    yield from run_server_in_thread(
        "standard",
        {
            "APD_SENSORS_API_KEY": "testing",
            "APD_SENSORS_DEPLOYMENT_ID": "a46b1d1207fd4cdcad39bbdf706dfe29",
        },
        12081,
    )


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
        assert mock_db_session.execute.call_count == len(sensors)
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
        assert mock_db_session.execute.call_count == len(sensors) * 2
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
        assert mock_db_session.execute.call_count == 2
        insertion_calls = mock_db_session.execute.call_args_list
        params = [call[0][0]._values for call in insertion_calls]
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
