"""
Bencode encoder/decoder for BitTorrent protocol.
Bencode is the encoding used by BitTorrent for storing and transmitting loosely structured data.
"""

import io
from collections import OrderedDict


class BencodeError(Exception):
    """Base exception for bencode errors."""
    pass


class BencodeDecodeError(BencodeError):
    """Exception raised when decoding fails."""
    pass


def encode(obj):
    """
    Encode a Python object into bencode format.
    
    Args:
        obj: Python object to encode (int, bytes, list, dict)
        
    Returns:
        bytes: Bencoded representation
    """
    if isinstance(obj, int):
        return f"i{obj}e".encode()
    elif isinstance(obj, bytes):
        return f"{len(obj)}:".encode() + obj
    elif isinstance(obj, str):
        obj_bytes = obj.encode('utf-8')
        return f"{len(obj_bytes)}:".encode() + obj_bytes
    elif isinstance(obj, list):
        result = b"l"
        for item in obj:
            result += encode(item)
        result += b"e"
        return result
    elif isinstance(obj, dict):
        result = b"d"
        # Sort keys to ensure consistent encoding
        for key in sorted(obj.keys()):
            if isinstance(key, str):
                key_bytes = key.encode('utf-8')
            else:
                key_bytes = key
            result += encode(key_bytes)
            result += encode(obj[key])
        result += b"e"
        return result
    else:
        raise TypeError(f"Unsupported type for bencode: {type(obj)}")


def decode(data):
    """
    Decode bencode data into Python objects.
    
    Args:
        data: bytes or file-like object containing bencoded data
        
    Returns:
        Decoded Python object
    """
    if isinstance(data, bytes):
        data = io.BytesIO(data)
    
    return _decode_recursive(data)


def _decode_recursive(data):
    """Recursively decode bencode data from a file-like object."""
    marker = data.read(1)
    if not marker:
        raise BencodeDecodeError("Unexpected end of data")
    
    if marker == b'i':
        # Integer
        return _decode_int(data)
    elif marker == b'l':
        # List
        return _decode_list(data)
    elif marker == b'd':
        # Dictionary
        return _decode_dict(data)
    elif marker.isdigit():
        # String
        return _decode_string(data, marker)
    else:
        raise BencodeDecodeError(f"Invalid marker: {marker}")


def _decode_int(data):
    """Decode a bencoded integer."""
    end_pos = data.tell()
    while True:
        char = data.read(1)
        if char == b'e':
            break
        if not char:
            raise BencodeDecodeError("Unexpected end of integer")
        end_pos = data.tell()
    
    data.seek(data.tell() - (end_pos - data.tell() + 1))
    int_str = data.read(end_pos - data.tell() - 1)
    data.read(1)  # Skip 'e'
    
    try:
        return int(int_str)
    except ValueError:
        raise BencodeDecodeError(f"Invalid integer: {int_str}")


def _decode_string(data, first_digit):
    """Decode a bencoded string."""
    # Read the length
    length_str = first_digit
    while True:
        char = data.read(1)
        if char == b':':
            break
        if not char or not char.isdigit():
            raise BencodeDecodeError("Invalid string length")
        length_str += char
    
    try:
        length = int(length_str)
    except ValueError:
        raise BencodeDecodeError(f"Invalid string length: {length_str}")
    
    # Read the string data
    string_data = data.read(length)
    if len(string_data) != length:
        raise BencodeDecodeError("Unexpected end of string")
    
    return string_data


def _decode_list(data):
    """Decode a bencoded list."""
    result = []
    while True:
        marker = data.read(1)
        if marker == b'e':
            break
        if not marker:
            raise BencodeDecodeError("Unexpected end of list")
        
        # Put the marker back
        data.seek(data.tell() - 1)
        result.append(_decode_recursive(data))
    
    return result


def _decode_dict(data):
    """Decode a bencoded dictionary."""
    result = OrderedDict()
    while True:
        marker = data.read(1)
        if marker == b'e':
            break
        if not marker:
            raise BencodeDecodeError("Unexpected end of dictionary")
        
        # Put the marker back
        data.seek(data.tell() - 1)
        
        # Decode key (must be a string)
        key = _decode_recursive(data)
        if not isinstance(key, bytes):
            raise BencodeDecodeError("Dictionary keys must be strings")
        
        # Decode value
        value = _decode_recursive(data)
        
        # Store with decoded key
        try:
            key_str = key.decode('utf-8')
        except UnicodeDecodeError:
            key_str = key
        
        result[key_str] = value
    
    return result


# Convenience functions
def loads(data):
    """Decode bencode data from bytes."""
    return decode(data)


def dumps(obj):
    """Encode object to bencode bytes."""
    return encode(obj)


# Testing
if __name__ == "__main__":
    # Test integer encoding/decoding
    assert encode(42) == b'i42e'
    assert decode(b'i42e') == 42
    assert encode(-42) == b'i-42e'
    assert decode(b'i-42e') == -42
    assert encode(0) == b'i0e'
    assert decode(b'i0e') == 0
    
    # Test string encoding/decoding
    assert encode(b'hello') == b'5:hello'
    assert decode(b'5:hello') == b'hello'
    assert encode('hello') == b'5:hello'
    
    # Test list encoding/decoding
    assert encode([1, b'hello', 2]) == b'li1e5:helloi2ee'
    assert decode(b'li1e5:helloi2ee') == [1, b'hello', 2]
    
    # Test dictionary encoding/decoding
    d = {'name': b'test', 'num': 42}
    encoded = encode(d)
    decoded = decode(encoded)
    assert decoded['name'] == b'test'
    assert decoded['num'] == 42
    
    # Test nested structures
    nested = {'list': [1, 2, {'inner': b'value'}], 'number': 100}
    encoded = encode(nested)
    decoded = decode(encoded)
    assert decoded['list'][2]['inner'] == b'value'
    assert decoded['number'] == 100
    
    print("All tests passed!")
