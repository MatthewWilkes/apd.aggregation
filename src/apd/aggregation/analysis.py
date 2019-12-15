from __future__ import annotations

import dataclasses
import datetime
import typing as t
from uuid import UUID

from matplotlib.axes._base import _AxesBase

from apd.aggregation.query import get_data_by_deployment
from apd.aggregation.database import DataPoint


@dataclasses.dataclass(frozen=True)
class Config:
    title: str
    sensor_name: str
    clean: t.Callable[
        [t.AsyncIterator[DataPoint]], t.AsyncIterator[t.Tuple[datetime.datetime, float]]
    ]
    ylabel: str


async def clean_magnitude(
    datapoints: t.AsyncIterator[DataPoint],
) -> t.AsyncIterator[t.Tuple[datetime.datetime, float]]:
    async for datapoint in datapoints:
        if datapoint.data is None:
            continue
        yield datapoint.collected_at, datapoint.data["magnitude"]


async def clean_passthrough(
    datapoints: t.AsyncIterator[DataPoint],
) -> t.AsyncIterator[t.Tuple[datetime.datetime, float]]:
    async for datapoint in datapoints:
        if datapoint.data is None:
            continue
        else:
            yield datapoint.collected_at, datapoint.data


configs = (
    Config(
        sensor_name="SolarCumulativeOutput",
        clean=clean_magnitude,
        title="Solar cumulative output",
        ylabel="Watt-hours",
    ),
    Config(
        sensor_name="RAMAvailable",
        clean=clean_passthrough,
        title="RAM available",
        ylabel="Bytes",
    ),
    Config(
        sensor_name="RelativeHumidity",
        clean=clean_passthrough,
        title="Relative humidity",
        ylabel="Percent",
    ),
    Config(
        sensor_name="Temperature",
        clean=clean_magnitude,
        title="Ambient temperature",
        ylabel="Degrees C",
    ),
)


def get_known_configs():
    return {config.title: config for config in configs}


async def plot_sensor(
    config: Config, plot: _AxesBase, location_names: t.Dict[UUID, str], **kwargs
):
    locations = []
    async for deployment, query_results in get_data_by_deployment(
        sensor_name=config.sensor_name, **kwargs
    ):
        # Mypy currently doesn't understand callable fields on datatypes: https://github.com/python/mypy/issues/5485
        points = [dp async for dp in config.clean(query_results)]  # type: ignore
        if not points:
            continue
        locations.append(deployment)
        x, y = zip(*points)
        plot.set_title(config.title)
        plot.set_ylabel(config.ylabel)
        plot.plot_date(x, y, f"-", xdate=True)
    plot.legend([location_names.get(l, l) for l in locations])
    return plot
