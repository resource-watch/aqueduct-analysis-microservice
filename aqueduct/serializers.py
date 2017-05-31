"""Serializers"""


def serialize_response(analysis):
    """."""
    return {
        'id': None,
        'type': 'water-risk-analysis',
        'wscheme': analysis.get('wscheme', None),
        'data': analysis.get('rows', None)
    }
