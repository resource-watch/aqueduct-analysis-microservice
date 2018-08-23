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
        'id': data.get('widget-id', None),
        'type': 'flood-cba-analysis',
        'params': data.get('params', None),
        'data': data.get('rows', None)
    }

def serialize_response_risk(data):
    """."""
    return {
        'id': data.get('widget-id', None),
        'type': 'flood-risk-analysis',
        'params': data.get('params', None),
        'data': data.get('rows', None)
    }
