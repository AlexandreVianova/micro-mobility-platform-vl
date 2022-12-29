import pytest
import pandas as pd
from bikeshare_data_processor.transformers import VelibDataTransformer, CapitalBikeShareDataTransformer


@pytest.fixture
def system_name():
    return 'capital-bikeshare'


@pytest.fixture
def capital_bike_share_raw_data():
    return pd.read_csv('test_datasets/raw_data_capital_bikeshare.csv', sep=',')


@pytest.fixture
def velib_raw_data():
    return pd.read_csv('test_datasets/raw_data_velib.csv', sep=',')


@pytest.mark.parametrize(
    'transformer,test_dataset', [
        (CapitalBikeShareDataTransformer, 'capital_bike_share_raw_data'),
        (VelibDataTransformer, 'velib_raw_data')
    ]
)
def test_prepare_stations_inventory(transformer, test_dataset, request):
    raw_data = request.getfixturevalue(test_dataset)
    output = transformer.prepare_stations_inventory(raw_data)
    assert output.shape[0] == raw_data.shape[0]


@pytest.mark.parametrize(
    'transformer,test_dataset', [
        (CapitalBikeShareDataTransformer, 'capital_bike_share_raw_data'),
        (VelibDataTransformer, 'velib_raw_data')
    ]
)
def test_prepare_stations_statuses(transformer, test_dataset, request):
    raw_data = request.getfixturevalue(test_dataset)
    output = transformer.prepare_stations_statuses(raw_data)
    assert output.shape[0] == raw_data.shape[0]
