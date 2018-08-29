"""Serializers"""


def serialize_response(analysis):
    """."""
    return {
        'id': None,
        'type': 'water-risk-analysis',
        'wscheme': analysis.get('wscheme', None),
        'data': analysis.get('rows', None)
    }

def serialize_response_cba(data):
    """."""
    return {
        'id':data.get("widgetId", None),
        'type': 'water-risk-analysis',
        'chart_type':data.get('chart_type', None),
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
