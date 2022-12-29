import pytest
import pandas as pd
from bikeshare_data_processor.ingestion_engine import IngestionEngine as Ie


@pytest.fixture
def system_name():
    return 'capital-bikeshare'


@pytest.fixture
def raw_statuses():
    return pd.read_csv('./test_datasets/raw_statuses.csv', sep=',')


def test_prepare_station_inventory(raw_statuses):
    output = Ie.prepare_station_inventory(raw_statuses)
    assert output.shape[0] == raw_statuses.shape[0]


def test_prepare_station_statuses_history(raw_statuses):
    output = Ie.prepare_stations_statuses(raw_statuses)
    assert output.shape[0] == raw_statuses.shape[0]
