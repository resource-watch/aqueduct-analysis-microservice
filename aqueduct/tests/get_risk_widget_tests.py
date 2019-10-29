import json
import os
import pytest

import aqueduct

@pytest.fixture
def client():
    if not os.getenv('CT_URL'):
        raise Exception('CT_URL needs to be set')
    if not os.getenv('CT_TOKEN'):
        raise Exception('CT_TOKEN needs to be set')
    if not os.getenv('POSTGRES_URL'):
        raise Exception('POSTGRES_URL needs to be set')

    app = aqueduct.app
    app.config['TESTING'] = True
    client = app.test_client()

    yield client


def test_get_analysis_happy_case(client):
    risk_widget_response_file = open(os.path.dirname(os.path.realpath(__file__)) + '/responses/risk_widget_response.json')
    risk_widget_response = json.load(risk_widget_response_file)

    response = client.get(
        '/api/v1/aqueduct/analysis/risk/widget/table?exposure=urban_damage_v2&flood=riverine&geogunit_unique_name=Netherlands&scenario=business%20as%20usual&sub_scenario=false')

    assert json.loads(response.data) == risk_widget_response
    assert response.status_code == 200

