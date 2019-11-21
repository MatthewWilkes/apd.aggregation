import asyncio
from concurrent.futures import ThreadPoolExecutor
import datetime
from unittest.mock import patch, MagicMock
import wsgiref.simple_server

import aiohttp
from apd.sensors.sensors import PythonVersion, ACStatus
from apd.sensors.wsgi import set_up_config
import flask
import pytest
import requests

from apd.sensors.wsgi import v20

from apd.aggregation import collect


@pytest.fixture(scope="module")
def sensors():
    """ Patch the get_sensors method to return a known pair of sensors only """
    data = [PythonVersion(), ACStatus()]
    with patch("apd.sensors.cli.get_sensors") as get_sensors:
        get_sensors.return_value = data
        yield data


def get_independent_flask_app(name):
    """ Create a new flask app with the v20 API blueprint loaded, so multiple copies
    of the app can be run in parallel without conflicting configuration """
    app = flask.Flask(name)
    app.register_blueprint(v20.version, url_prefix="/v/2.0")
    return app


def run_server_in_thread(name, config, port):
    # Keep a shared reference to allow the main thread to stop the worker thread
    running = True

    def set_up_and_serve():
        app = get_independent_flask_app(
            name
        )  # Create a new flask app and load in required code, to prevent config conflicts
        flask_app = set_up_config(config, app)
        with wsgiref.simple_server.make_server("localhost", port, flask_app) as server:
            while running:
                server.handle_request()  # Process requests one-by-one to allow the main thread to stop the loop

    with ThreadPoolExecutor() as pool:
        pool.submit(set_up_and_serve)
        yield
        running = False
        # Make a final request, to cause the loop to re-evaluate the running condition
        requests.get(f"http://localhost:{port}")


@pytest.fixture(scope="module")
def http_server(sensors):
    yield from run_server_in_thread(
        "standard", {"APD_SENSORS_API_KEY": "testing"}, 12081
    )


@pytest.fixture(scope="module")
def bad_api_key_http_server(sensors):
    yield from run_server_in_thread(
        "alternate", {"APD_SENSORS_API_KEY": "penny"}, 12082
    )


class TestGetDataPoints:
    @pytest.fixture
    def mut(self):
        return collect.get_data_points

    def test_get_data_points(self, sensors, mut, http_server):
        async def wrapped():
            async with aiohttp.ClientSession() as http:
                return await mut("http://localhost:12081/", "testing", http)

        # Get the data from the server, storing the time before and after
        # as bounds for the collected_at value
        time_before = datetime.datetime.now()
        results = asyncio.run(wrapped())
        time_after = datetime.datetime.now()

        assert len(results) == len(sensors) == 2

        for (sensor, result) in zip(sensors, results):
            assert sensor.from_json_compatible(result.data) == sensor.value()
            assert result.sensor_name == sensor.name
            assert time_before <= result.collected_at <= time_after

    def test_get_data_points_fails_with_bad_api_key(self, sensors, mut, http_server):
        async def wrapped():
            async with aiohttp.ClientSession() as http:
                return await mut("http://localhost:12081/", "incorrect", http)

        with pytest.raises(
            ValueError,
            match="Error loading data from http://localhost:12081/: Supply API key in X-API-Key header",
        ):
            asyncio.run(wrapped())


class TestAddDataFromSensors:
    @pytest.fixture
    def mut(self):
        return collect.add_data_from_sensors

    @pytest.fixture
    def mock_db_session(self):
        return MagicMock()

    def test_get_get_data_from_sensors(
        self, mock_db_session, sensors, mut, http_server
    ):
        async def wrapped():
            return await mut(mock_db_session, ["http://localhost:12081/"], "testing")

        results = asyncio.run(wrapped())
        assert mock_db_session.add.call_count == len(sensors)
        assert len(results) == len(sensors)

    def test_get_get_data_from_sensors_with_multiple_servers(
        self, mock_db_session, sensors, mut, http_server
    ):
        async def wrapped():
            return await mut(
                mock_db_session,
                ["http://localhost:12081/", "http://localhost:12081/"],
                "testing",
            )

        results = asyncio.run(wrapped())
        assert mock_db_session.add.call_count == len(sensors) * 2
        assert len(results) == len(sensors) * 2

    def test_data_points_not_added_if_only_partial_success(
        self, mock_db_session, sensors, mut, http_server, bad_api_key_http_server
    ):
        async def wrapped():
            return await mut(
                mock_db_session,
                ["http://localhost:12081/", "http://localhost:12082/"],
                "testing",
            )

        with pytest.raises(
            ValueError,
            match="Error loading data from http://localhost:12082/: Supply API key in X-API-Key header",
        ):
            asyncio.run(wrapped())
        assert mock_db_session.add.call_count == 0
