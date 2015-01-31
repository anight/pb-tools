
class Truncated(Exception): pass
class DecodeError(Exception): pass

def decode(read):
	state = "reading_0x78"
	msg_id = 0
	msg_len = 0
	shift = 0

	def smart_read(bytes, ok_zero=False):
		ret = read(bytes)
		if len(ret) < bytes:
			if not ok_zero or len(ret) != 0:
				raise Truncated()
		return ret

	while True:
		if state == "reading_0x78":
			byte = smart_read(1, ok_zero=True)
			if len(byte) == 0:
				return # EOF
			if ord(byte) != 0x78:
				raise DecodeError("unexpected byte 0x%02x state %s" % (ord(byte), state))
			state = "reading_msg_id"
		elif state == "reading_msg_id":
			byte = smart_read(1)
			msg_id |= (ord(byte) & 0x7f) << shift
			if 0 == ord(byte) & 0x80:
				state = "reading_0x72"
				shift = 0
			else:
				shift += 7
				if shift >= 32:
					raise DecodeError("too long varint")
		elif state == "reading_0x72":
			byte = smart_read(1)
			if ord(byte) != 0x72:
				raise DecodeError("unexpected byte 0x%02x state %s" % (ord(byte), state))
			state = "reading_msg_len"
		elif state == "reading_msg_len":
			byte = smart_read(1)
			msg_len |= (ord(byte) & 0x7f) << shift
			if 0 == ord(byte) & 0x80:
				state = "reading_body"
				shift = 0
			else:
				shift += 7
				if shift >= 32:
					raise DecodeError("too long varint")
		elif state == "reading_body":
			body = ''
			if msg_len > 0:
				body = smart_read(msg_len)
			yield msg_id, body
			state = "reading_0x78"
			msg_id = 0
			msg_len = 0
