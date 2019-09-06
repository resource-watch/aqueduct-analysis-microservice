"""Serializers"""


def serialize_response(analysis):
    """."""
    return {
        'id': None,
        'type': 'water-risk-analysis',
        'analysis_type': analysis.get('analysis_type', None),
        'wscheme': analysis.get('wscheme', None),
        'month': analysis.get('month', None),
        'year': analysis.get('year', None),
        'change_type': analysis.get('change_type', None),
        'indicator': analysis.get('indicator', None),
        'scenario': analysis.get('scenario', None),
        'downloadUrl': analysis.get('downloadUrl', None),
        'data': analysis.get('rows', None)
    }


def serialize_response_geocoding(data):
    """."""
    return {'rows': data.to_dict(orient='record')}


def serialize_response_cba(data):
    """."""
    return {
        'id': data.get("widgetId", None),
        'type': 'water-risk-analysis',
        'chart_type': data.get('chart_type', None),
        'meta': data.get("meta", None),
        'data': data.get('data', None)
    }


def serialize_response_default(data):
    """."""
    return {
        'type': 'water-risk-cba-default',
        'data': data
    }


def serialize_response_risk(data):
    """."""
    return data
