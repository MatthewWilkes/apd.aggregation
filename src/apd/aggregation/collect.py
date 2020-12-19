import asyncio
from contextvars import ContextVar
import datetime
import logging
import typing as t
import uuid

import aiohttp
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session
from sqlalchemy.dialects.postgresql import insert

from .database import DataPoint, datapoint_table, Deployment, deployment_table

http_session_var: ContextVar[aiohttp.ClientSession] = ContextVar("http_session")

logger = logging.getLogger(__name__)


async def get_deployment_id(server):
    http = http_session_var.get()
    if not server.endswith("/"):
        server += "/"
    url = server + "v/3.1/deployment_id"
    try:
        async with http.get(url) as request:
            if request.status != 200:
                raise ValueError(f"Error loading deployment id from {server}")
            result = await request.json()
            return uuid.UUID(result["deployment_id"])
    except aiohttp.ClientError as err:
        raise ValueError(f"Error loading deployment id from {server}") from err


async def get_data_points(
    server: str,
    api_key: t.Optional[str],
) -> t.List[DataPoint]:
    if not server.endswith("/"):
        server += "/"
    url = server + "v/3.1/sensors/"
    headers = {}
    if api_key:
        headers["X-API-KEY"] = api_key
    http = http_session_var.get()

    # Get the deployment ID in parallel to the sensor data
    deployment_id_task = asyncio.create_task(get_deployment_id(server))

    try:
        async with http.get(url, headers=headers) as request:
            ok = request.status == 200
            try:
                result = await request.json()
            except aiohttp.ContentTypeError:
                raise ValueError(
                    f"Error loading data from {server}: Server response {await request.text()}"
                )
    except aiohttp.ClientError as err:
        # The HTTP request to get the sensor data failed
        raise ValueError(f"Error loading data from {server}") from err

    now = datetime.datetime.now()
    if ok:
        points = []
        deployment_id = await deployment_id_task
        for value in result["sensors"]:
            points.append(
                DataPoint(
                    sensor_name=value["id"],
                    collected_at=now,
                    data=value["value"],
                    deployment_id=deployment_id,
                )
            )
        return points
    else:
        raise ValueError(
            f"Error loading data from {server}: " + result.get("error", "Unknown")
        )


async def get_known_sensors(server: str, api_key: t.Optional[str]) -> t.List[str]:
    if not server.endswith("/"):
        server += "/"
    url = server + "v/3.1/info/sensors"
    headers = {}
    if api_key:
        headers["X-API-KEY"] = api_key
    http = http_session_var.get()

    try:
        async with http.get(url, headers=headers) as request:
            ok = request.status == 200
            try:
                result = await request.json()
            except aiohttp.ContentTypeError:
                raise ValueError(
                    f"Error loading data from {server}: Server response {await request.text()}"
                )
    except aiohttp.ClientError as err:
        # The HTTP request to get the sensor data failed
        raise ValueError(f"Error loading data from {server}") from err

    if ok:
        return result
    else:
        raise ValueError(
            f"Error loading data from {server}: " + result.get("error", "Unknown")
        )


async def get_historical_data_points_since(
    server: str,
    api_key: t.Optional[str],
    since: datetime.datetime,
    sensor_name: str,
) -> t.List[DataPoint]:
    if not server.endswith("/"):
        server += "/"
    url = server + f"v/3.1/sensors/{sensor_name}/historical/{since.date().isoformat()}"
    headers = {}
    if api_key:
        headers["X-API-KEY"] = api_key
    http = http_session_var.get()

    # Get the deployment ID in parallel to the sensor data
    deployment_id_task = asyncio.create_task(get_deployment_id(server))

    try:
        async with http.get(url, headers=headers) as request:
            ok = request.status == 200
            try:
                result = await request.json()
            except aiohttp.ContentTypeError:
                raise ValueError(
                    f"Error loading data from {server}: Server response {await request.text()}"
                )
    except aiohttp.ClientError as err:
        # The HTTP request to get the sensor data failed
        raise ValueError(f"Error loading data from {server}") from err

    if ok:
        points = []
        deployment_id = await deployment_id_task
        for value in result["sensors"]:
            if value["collected_at"] < since.isoformat():
                continue
            points.append(
                DataPoint(
                    sensor_name=value["id"],
                    collected_at=value["collected_at"],
                    data=value["value"],
                    deployment_id=deployment_id,
                )
            )
        return points
    else:
        raise ValueError(
            f"Error loading data from {server}: " + result.get("error", "Unknown")
        )


def handle_result(result: t.List[DataPoint], session: Session) -> t.List[DataPoint]:
    for point in result:
        insert_stmt = (
            insert(datapoint_table)
            .values(**point._asdict())
            .on_conflict_do_update(
                constraint="unique_reading", set_={"data": point.data}
            )
        )
        sql_result = session.execute(insert_stmt)
        point.id = sql_result.inserted_primary_key[0]
    return result


def most_recent_data_point_for_sensor_and_server(
    server: uuid.UUID, sensor_name: str, session: Session
) -> t.Optional[datetime.datetime]:
    get = (
        datapoint_table.select()
        .where(
            datapoint_table.c.deployment_id == server,
            datapoint_table.c.sensor_name == sensor_name,
        )
        .order_by(datapoint_table.c.collected_at.desc())
        .limit(1)
    )
    sql_result = session.execute(get)
    value = sql_result.first()
    if value is None:
        return None
    else:
        value = value["collected_at"]
    if isinstance(value, datetime.datetime):
        return value
    else:
        return None


async def add_data_from_sensors(
    session: Session, servers: t.Iterable[Deployment]
) -> t.List[DataPoint]:
    tasks: t.List[t.Awaitable[t.List[DataPoint]]] = []
    points: t.List[DataPoint] = []
    loop = asyncio.get_running_loop()

    async with aiohttp.ClientSession() as http:
        http_session_var.set(http)
        tasks = [get_data_points(server.uri, server.api_key) for server in servers]
        for server in servers:
            try:
                sensors_on_server = await get_known_sensors(server.uri, server.api_key)
            except ValueError:
                continue
            for sensor_name in sensors_on_server:
                latest = await loop.run_in_executor(
                    None,
                    most_recent_data_point_for_sensor_and_server,
                    server.id,
                    sensor_name,
                    session,
                )
                if not latest:
                    latest = datetime.datetime.now() - datetime.timedelta(days=28)
                tasks.append(
                    get_historical_data_points_since(
                        server.uri,
                        server.api_key,
                        latest + datetime.timedelta(minutes=1),
                        sensor_name,
                    )
                )

        for results in await asyncio.gather(*tasks, return_exceptions=True):
            if isinstance(results, Exception):
                # This server failed, log the error and continue
                logger.error("Data retrieval failed", exc_info=results)
                continue
            points += results
    await loop.run_in_executor(None, handle_result, points, session)
    return points


def standalone(
    db_uri: str, servers: t.Tuple[str], api_key: t.Optional[str], echo: bool = False
) -> None:
    engine = create_engine(db_uri, echo=echo)
    sm = sessionmaker(engine)
    Session = sm()

    deployments: t.Iterable[Deployment]
    if servers:
        # Explicit servers have been passed, create temporary deployment objects to
        # represent them
        deployments = tuple(
            Deployment(id=None, name=None, colour=None, api_key=api_key, uri=server)
            for server in servers
        )
    else:
        # Get the deployments stored in the database
        deployment_data = Session.query(deployment_table).all()
        deployments = tuple(
            Deployment.from_sql_result(deployment) for deployment in deployment_data
        )

    # Start the event loop and add the from the collected deployments to the session
    asyncio.run(add_data_from_sensors(Session, deployments))

    if "postgresql" in db_uri:
        # On Postgres sent a pubsub notification, in case other processes are waiting
        # for this data.
        # This won't be sent until the session is committed.
        Session.execute("NOTIFY apd_aggregation;")

    Session.commit()


if __name__ == "__main__":
    standalone(
        db_uri="postgresql+psycopg2://apd@localhost/apd",
        servers=("http://pvoutput:8080/",),
        api_key="h3hdfjksfhwkjehnwekj",
    )
