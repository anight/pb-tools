#! /usr/bin/python

# (c) 2010, Andrei Nigmatulin

import os, socket, struct
from protobuf_json import json2pb

import time

def retry_once_on(e):

	def deco_retry(f):
		def f_retry(*args, **kwargs):

			try:
				return f(*args, **kwargs)
			except e:
				return f(*args, **kwargs)

		return f_retry
	return deco_retry

class PBService:

	class _IOFailed(Exception): pass

	def __init__(self, **kvargs):
		if 'unix_socket' in kvargs:
			self._family = socket.AF_UNIX
			self._addr = kvargs['unix_socket']
		else:
			self._family = socket.AF_INET
			self._addr = (kvargs['host'], int(kvargs['port']))
		if 'connect_timeout' in kvargs:
			self._connect_timeout = kvargs['connect_timeout']
		else:
			self._connect_timeout = 30
		if 'io_timeout' in kvargs:
			self._io_timeout = kvargs['io_timeout']
		else:
			self._io_timeout = 60

		self.proto = __import__(kvargs['proto'] + '_pb2')
		self._connected = False
		self._has_more = False

	def _connect(self):
		if self._connected:
			return
		self._sock = socket.socket(self._family, socket.SOCK_STREAM)

		self._sock.settimeout(self._connect_timeout)
		err = self._sock.connect_ex(self._addr)
		if err != 0:
			raise self._IOFailed("can't connect = %d, %s" % (err, os.strerror(err)))
		self._connected = True
		self._sock.settimeout(self._io_timeout)

	def _recv_n(self, n):
		buf = ''

		while (len(buf) < n):
			chunk = self._sock.recv(n - len(buf))
			if chunk == '':
				break
			buf = buf + chunk

		if len(buf) != n:
			self._sock.close()
			self._connected = False
			raise self._IOFailed("Truncated response: received %d bytes from %d expected" % (len(buf), n))

		return buf

	def _send(self, bytes):
		try:
			self._sock.sendall(bytes)
		except socket.error:
			self._sock.close()
			self._connected = False
			raise self._IOFailed("Send failed")

	@retry_once_on(_IOFailed) # connect, recv or send
	def _pb2_call(self, req):

		""" Hides service i/o, message ids and other binary protocol stuff """

		assert not self._has_more

		self._connect()

		req_msgid = self.proto._REQUEST_MSGID.values_by_name[req.DESCRIPTOR.name.upper()]
		bin_data = req.SerializeToString()
		payload = struct.pack('!II', len(bin_data) + 4, req_msgid.number) + bin_data

		self._send(payload)

		return self._read_packet()

	def _read_packet(self):
		buf = self._recv_n(4)
		res_len = struct.unpack('!I', buf)[0]

		buf = self._recv_n(res_len)
		res_msgid = struct.unpack('!I', buf[0:4])[0]
		res_body = buf[4:]

		if res_msgid & 0x80000000:
			res_msgid &= ~0x80000000
			self._has_more = True
		else:
			self._has_more = False
		res_msg = self.proto._RESPONSE_MSGID.values_by_number[res_msgid]
		res = getattr(self.proto, res_msg.name.lower())()
		res.ParseFromString(res_body)

		return res

	def __getattr__(self, name):
		def call(*a, **kv):
			if len(a): # arg passed as pb2 object
				req_pb2 = a[0]
			else: # arg passed as dict
				req_pb2 = getattr(self.proto, 'request_%s' % name)()
				json2pb(req_pb2, kv)
			o = self._pb2_call(req_pb2)
			if o.DESCRIPTOR.name == 'response_generic' and o.error_code != 0:
				self._service_error = o
			else:
				self._service_error = None
			return o
		return call

if __name__ == '__main__':

	""" Usage example """

	mm = PBService(host='127.0.0.1', port=11013, proto='meetmaker')
	print mm.user_get(user_id=123)

