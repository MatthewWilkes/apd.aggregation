import pytest

from apd.aggregation.query import (
    get_data,
    db_session_var,
)


@pytest.mark.usefixtures("populated_db")
class TestGetData:
    @pytest.fixture
    def mut(self):
        return get_data

    @pytest.mark.asyncio
    async def test_iterate_over_items(self, mut, db_session):
        db_session_var.set(db_session)
        points = [dp async for dp in mut()]
        assert len(points) == 9
