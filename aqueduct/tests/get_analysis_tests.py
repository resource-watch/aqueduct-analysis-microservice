import json
import os
import urllib.parse
import requests_mock

from RWAPIMicroservicePython.test_utils import mock_request_validation


@requests_mock.mock(kw="mocker")
def test_get_analysis_with_non_existent_geostore(client, mocker):
    mock_request_validation(mocker, microservice_token=os.getenv("MICROSERVICE_TOKEN"))
    mocker.get(
        f"{os.getenv('GATEWAY_URL')}/v1/geostore/d58f8840f09553e57b31fe0bad51df33",
        status_code=404,
        request_headers={
            "x-api-key": "api-key-test",
        },
    )

    response = client.get(
        "/api/v1/aqueduct/analysis?analysis_type=custom&wscheme=%27[0.,0.25,0.5,1.,1.,1.,null,1.,1.,0.25,1.,1.,1.]%27&geostore=d58f8840f09553e57b31fe0bad51df33",
        headers={"x-api-key": "api-key-test"},
    )
    assert response.json == {
        "errors": [
            {
                "detail": "Geostore not found: Could not reach geostore service",
                "status": 404,
            }
        ]
    }
    assert response.status_code == 404


@requests_mock.mock(kw="mocker")
def test_get_analysis_invalid_geostore_type(client, mocker):
    with open(
        os.path.dirname(os.path.realpath(__file__))
        + "/responses/geostore_multipolygon.json"
    ) as geostore_data_file:
        geostore_data = json.load(geostore_data_file)

        mock_request_validation(
            mocker, microservice_token=os.getenv("MICROSERVICE_TOKEN")
        )
        mocker.get(
            f"{os.getenv('GATEWAY_URL')}/v1/geostore/d58f8840f09553e57b31fe0bad51df33",
            json=geostore_data,
            request_headers={
                "x-api-key": "api-key-test",
            },
        )

        response = client.get(
            "/api/v1/aqueduct/analysis?analysis_type=custom&wscheme=%27[0.,0.25,0.5,1.,1.,1.,null,1.,1.,0.25,1.,1.,1.]%27&geostore=d58f8840f09553e57b31fe0bad51df33",
            headers={"x-api-key": "api-key-test"},
        )
        assert json.loads(response.data) == {
            "errors": [
                {
                    "detail": "Error: geostore must be of multipoint type, not MultiPolygon.",
                    "status": 500,
                }
            ]
        }
        assert response.status_code == 500


@requests_mock.mock(kw="mocker")
def test_get_analysis_happy_case(client, mocker):
    geostore_data_file = open(
        os.path.dirname(os.path.realpath(__file__))
        + "/responses/geostore_multipoint.json"
    )
    geostore_data = json.load(geostore_data_file)

    analysis_response_file = open(
        os.path.dirname(os.path.realpath(__file__))
        + "/responses/analysis_happy_case_response.json"
    )
    analysis_response = json.load(analysis_response_file)

    mock_request_validation(mocker, microservice_token=os.getenv("MICROSERVICE_TOKEN"))
    mocker.get(
        f"{os.getenv('GATEWAY_URL')}/v1/geostore/d58f8840f09553e57b31fe0bad51df33",
        json=geostore_data,
        request_headers={
            "x-api-key": "api-key-test",
        },
    )

    post_sql_query = "SELECT * FROM get_aqpoints_annual_custom_test('[0, 1, 2]', '[0.,0.25,0.5,1.,1.,1.,null,1.,1.,0.25,1.,1.,1.]', '[''Point(-77.007986 38.898992)'', ''Point(-46.693983 -23.568232)'', ''Point(4.318852 52.07906)'']', '[null, null, null]', '[null, null, null]', '[null, null, null]')"
    post_response = {
        "rows": [
            {
                "points_id": 2,
                "location_name": None,
                "input_address": None,
                "match_address": None,
                "latitude": 52.07906,
                "longitude": 4.318852,
                "major_basin_name": "Rhine",
                "minor_basin_name": "Nieuwe Mass",
                "aquifer_name": None,
                "string_id": "232610-NLD.14_1-883",
                "aq30_id": 18209,
                "gid_1": "NLD.14_1",
                "gid_0": "NLD",
                "name_0": "Netherlands",
                "name_1": "Zuid-Holland",
                "raw": 0.705634561519841,
                "label": "Low",
                "the_geom": "0101000020E6100000880E812381461140FC3559A31E0A4A40",
            },
            {
                "points_id": 1,
                "location_name": None,
                "input_address": None,
                "match_address": None,
                "latitude": -23.568232,
                "longitude": -46.693983,
                "major_basin_name": "La Plata",
                "minor_basin_name": "Tiete 2",
                "aquifer_name": None,
                "string_id": "642789-BRA.25_1-2485",
                "aq30_id": 49401,
                "gid_1": "BRA.25_1",
                "gid_0": "BRA",
                "name_0": "Brazil",
                "name_1": "São Paulo",
                "raw": 2.22050231107011,
                "label": "Medium - High",
                "the_geom": "0101000020E6100000707D586FD45847C0698A00A7779137C0",
            },
            {
                "points_id": 0,
                "location_name": None,
                "input_address": None,
                "match_address": None,
                "latitude": 38.898992,
                "longitude": -77.007986,
                "major_basin_name": "United States, North Atlantic Coast",
                "minor_basin_name": "Middle Potomac / Anacostia / Occoquan",
                "aquifer_name": "Gulf Coastal Plains Aquifer System",
                "string_id": "731700-USA.9_1-1400",
                "aq30_id": 53727,
                "gid_1": "USA.9_1",
                "gid_0": "USA",
                "name_0": "United States",
                "name_1": "District of Columbia",
                "raw": 0.942518811462127,
                "label": "Low",
                "the_geom": "0101000020E6100000DB34B6D7824053C0CDAE7B2B12734340",
            },
        ],
        "time": 1.223,
        "fields": {
            "points_id": {"type": "number", "pgtype": "int4"},
            "location_name": {"type": "string", "pgtype": "text"},
            "input_address": {"type": "string", "pgtype": "text"},
            "match_address": {"type": "string", "pgtype": "text"},
            "latitude": {"type": "number", "pgtype": "numeric"},
            "longitude": {"type": "number", "pgtype": "numeric"},
            "major_basin_name": {"type": "string", "pgtype": "text"},
            "minor_basin_name": {"type": "string", "pgtype": "text"},
            "aquifer_name": {"type": "string", "pgtype": "text"},
            "string_id": {"type": "string", "pgtype": "text"},
            "aq30_id": {"type": "number", "pgtype": "numeric"},
            "gid_1": {"type": "string", "pgtype": "text"},
            "gid_0": {"type": "string", "pgtype": "text"},
            "name_0": {"type": "string", "pgtype": "text"},
            "name_1": {"type": "string", "pgtype": "text"},
            "raw": {"type": "number", "pgtype": "numeric"},
            "label": {"type": "string", "pgtype": "text"},
            "the_geom": {"type": "geometry", "dims": 4, "srid": -1},
        },
        "total_rows": 3,
    }

    post_calls = mocker.post("https://wri-rw.carto.com/api/v2/sql", json=post_response)

    response = client.get(
        "/api/v1/aqueduct/analysis?analysis_type=custom&wscheme=%27[0.,0.25,0.5,1.,1.,1.,null,1.,1.,0.25,1.,1.,1.]%27&geostore=d58f8840f09553e57b31fe0bad51df33",
        headers={"x-api-key": "api-key-test"},
    )

    assert post_calls.call_count == 1
    assert post_calls.called
    assert post_calls.last_request.text == "q=" + urllib.parse.quote_plus(
        post_sql_query
    )

    assert json.loads(response.data) == analysis_response
    assert response.status_code == 200
