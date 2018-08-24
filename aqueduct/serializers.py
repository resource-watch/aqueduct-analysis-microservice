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
    return data

def serialize_response_risk(data):
    """."""
    return data
