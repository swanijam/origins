import io
import json


def _serialize(writer, records, _bytes=False):
    if _bytes:
        for record in records:
            writer.write(json.dumps(record).encode('utf-8'))
            writer.write(b'\n')
    else:
        for record in records:
            writer.write(json.dumps(record))
            writer.write('\n')


def serialize(records, buf):
    "Takes a list of records and serializes it into a file-like object."
    if isinstance(buf, io.StringIO):
        _serialize(buf, records)
    elif isinstance(buf, io.BytesIO):
        _serialize(buf, records, _bytes=True)
    elif isinstance(buf, str):
        with open(buf, 'w') as f:
            _serialize(f, records)


def _deserialize(line, _bytes=False):
    if _bytes:
        return json.loads(line.decode('utf-8'))

    return json.loads(line)


def deserialize(buf):
    "Takes a file-like object and deserializes the contents into records."
    if isinstance(buf, io.StringIO):
        for line in buf:
            yield _deserialize(line)
    elif isinstance(buf, io.BytesIO):
        for line in buf:
            yield _serialize(line, _bytes=True)
