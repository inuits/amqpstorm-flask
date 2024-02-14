import json
from datetime import datetime, timezone


class CustomJSONEncoder(json.JSONEncoder):
    def __convert_datetime(self, obj):
        if obj.tzinfo is None:
            obj = obj.astimezone(timezone.utc)
        return obj.isoformat()

    def default(self, obj):
        if not isinstance(obj, datetime):
            return super().default(obj)
        return self.__convert_datetime(obj)

    def encode(self, obj):
        if not isinstance(obj, datetime):
            return super().encode(obj)
        return self.__convert_datetime(obj)
