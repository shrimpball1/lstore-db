def int_to_8_bytes(number: int):
    return int(number).to_bytes(8, byteorder='big')


def eight_bytes_to_int(bytes: bytes | bytearray):
    return int.from_bytes(bytes, 'big')
