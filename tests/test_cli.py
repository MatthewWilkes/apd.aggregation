import datetime
import mock
import uuid

from click.testing import CliRunner
import pytest

from apd.aggregation import collect
import apd.aggregation.cli
from apd.aggregation.database import Deployment, deployment_table


@pytest.mark.functional
def test_sensors_are_passed_to_get_data_points(db_uri):
    runner = CliRunner()
    with mock.patch("apd.aggregation.collect.get_data_points") as get_data_points:
        runner.invoke(
            apd.aggregation.cli.collect_sensor_data,
            [
                "http://localhost",
                "http://otherhost",
                "--db",
                db_uri,
                "--api-key",
                "key",
            ],
        )
        calls = get_data_points.call_args_list
        assert len(calls) == 2
        details = {call[0] for call in get_data_points.call_args_list}
        assert details == {
            ("http://localhost", "key"),
            ("http://otherhost", "key"),
        }


@pytest.mark.functional
def test_add_deployment_to_db(db_uri, db_session, migrated_db):
    runner = CliRunner()
    deployment_id = uuid.UUID("76587cdd42dc489ea1d220e9b0bc62ca")
    with mock.patch("apd.aggregation.collect.get_deployment_id") as get_deployment_id:
        get_deployment_id.return_value = deployment_id
        runner.invoke(
            apd.aggregation.cli.deployments,
            ["add", "http://otherhost", "Other", "--db", db_uri, "--api-key", "key"],
        )
    deployments = db_session.query(deployment_table).all()
    assert len(deployments) == 1
    deployment = Deployment.from_sql_result(deployments[0])
    assert deployment.uri == "http://otherhost"
    assert deployment.api_key == "key"
    assert deployment.id == deployment_id


@pytest.mark.functional
def test_use_stored_deployments(db_uri, migrated_db):
    runner = CliRunner()

    deployment_id = uuid.UUID("76587cdd42dc489ea1d220e9b0bc62ca")
    with mock.patch("apd.aggregation.collect.get_deployment_id") as get_deployment_id:
        get_deployment_id.return_value = deployment_id
        runner.invoke(
            apd.aggregation.cli.deployments,
            [
                "add",
                "http://specifiedhost",
                "Specified",
                "--db",
                db_uri,
                "--api-key",
                "an_api_key",
            ],
        )

    with mock.patch("apd.aggregation.collect.get_data_points") as get_data_points:
        runner.invoke(
            apd.aggregation.cli.collect_sensor_data,
            ["--db", db_uri],
        )
        calls = get_data_points.call_args_list
        assert len(calls) == 1
        details = {call[0] for call in get_data_points.call_args_list}
        assert details == {
            ("http://specifiedhost", "an_api_key"),
        }


@pytest.mark.functional
def test_list_deployments(db_uri, migrated_db):
    runner = CliRunner()

    deployment_id = uuid.UUID("76587cdd42dc489ea1d220e9b0bc62ca")
    with mock.patch("apd.aggregation.collect.get_deployment_id") as get_deployment_id:
        get_deployment_id.return_value = deployment_id
        runner.invoke(
            apd.aggregation.cli.deployments,
            [
                "add",
                "http://specifiedhost",
                "Specified",
                "--db",
                db_uri,
                "--api-key",
                "an_api_key",
            ],
        )

    result = runner.invoke(
        apd.aggregation.cli.deployments,
        ["list", "--db", db_uri],
    )
    expected = """Specified
ID 76587cdd42dc489ea1d220e9b0bc62ca
URI http://specifiedhost
API key an_api_key
Colour None

"""
    assert result.output == expected


@pytest.mark.functional
def test_edit_deployment(db_uri, migrated_db):
    runner = CliRunner()
    return
    deployment_id = uuid.UUID("76587cdd42dc489ea1d220e9b0bc62ca")
    with mock.patch("apd.aggregation.collect.get_deployment_id") as get_deployment_id:
        get_deployment_id.return_value = deployment_id
        runner.invoke(
            apd.aggregation.cli.deployments,
            [
                "add",
                "http://specifiedhost",
                "Specified",
                "--db",
                db_uri,
                "--api-key",
                "an_api_key",
            ],
        )

    result = runner.invoke(
        apd.aggregation.cli.deployments,
        [
            "edit",
            "--db",
            db_uri,
            deployment_id.hex(),
            "--colour",
            "red" "--name",
            "New name",
        ],
    )
    expected = """New name
ID 76587cdd42dc489ea1d220e9b0bc62ca
URI http://specifiedhost
API key an_api_key
Colour red

"""
    assert result.output == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "sensor_name,time",
    (("Test", datetime.datetime(2020, 4, 1, 12, 3, 1)), ("Wrong", None)),
)
async def test_find_most_recent_result(populated_db, db_session, sensor_name, time):
    deployment_id = uuid.UUID("b4c68905-b1e4-4875-940e-69e5d27730fd")
    value = collect.most_recent_data_point_for_sensor_and_server(
        deployment_id, sensor_name, db_session
    )
    assert value == time
