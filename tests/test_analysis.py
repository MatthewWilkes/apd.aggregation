import datetime
import uuid

import pytest

from apd.aggregation import analysis
from apd.aggregation.database import DataPoint


async def generate_datapoints(datas):
    deployment_id = uuid.uuid4()
    for i, (time, data) in enumerate(datas, start=1):
        yield DataPoint(
            id=i,
            collected_at=time,
            sensor_name="TestSensor",
            data=data,
            deployment_id=deployment_id,
        )


class TestPassThroughCleaner:
    @pytest.fixture
    def cleaner(self):
        return analysis.clean_passthrough

    @pytest.mark.asyncio
    async def test_float_passthrough(self, cleaner):
        data = [
            (datetime.datetime(2020, 4, 1, 12, 0, 0), 65.0),
            (datetime.datetime(2020, 4, 1, 13, 0, 0), 65.5),
        ]
        datapoints = generate_datapoints(data)
        output = [(time, data) async for (time, data) in cleaner(datapoints)]
        assert output == data

    @pytest.mark.asyncio
    async def test_Nones_are_skipped(self, cleaner):
        data = [
            (datetime.datetime(2020, 4, 1, 12, 0, 0), None),
            (datetime.datetime(2020, 4, 1, 13, 0, 0), 65.5),
        ]
        datapoints = generate_datapoints(data)
        output = [(time, data) async for (time, data) in cleaner(datapoints)]
        assert output == [
            (datetime.datetime(2020, 4, 1, 13, 0, 0), 65.5),
        ]
