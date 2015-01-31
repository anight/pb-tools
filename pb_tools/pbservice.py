#! /usr/bin/python

# (c) 2010-2015, Andrei Nigmatulin

import os
import socket
import struct
import asyncore
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

class IOFailed(Exception):
	pass

class IncorrectUse(Exception):
	pass

class common(object):

	def __init__(self, kvargs):
		self._sock = None
		self._own_socket = True
		self.proto = __import__(kvargs['proto'] + '_pb2')
		self._io_timeout = 60

		if 'socket' in kvargs:
			self._sock = kvargs['socket']
			self._own_socket = False
		else:
			if 'unix_socket' in kvargs:
				self._family = socket.AF_UNIX
				self._addr = kvargs['unix_socket']
			else:
				self._family = socket.AF_INET
				self._addr = (kvargs['host'], int(kvargs['port']))

		if 'io_timeout' in kvargs:
			self._io_timeout = kvargs['io_timeout']

	def _clean_close(self):
		if self._own_socket:
			self._sock.close()
			self._sock = None

	def _recv_n(self, n):
		buf = ''

		while (len(buf) < n):
			chunk = self._sock.recv(n - len(buf))
			if chunk == '':
				break
			buf += chunk

		if len(buf) != n:
			self._clean_close()
			raise IOFailed("Truncated response: received %d bytes from %d expected" % (len(buf), n))

		return buf

	def _send(self, bytes):
		try:
			self._sock.sendall(bytes)
		except socket.error:
			self._clean_close()
			raise IOFailed("Send failed")

	def _encode_msg(self, msg):
		if msg.DESCRIPTOR.name.startswith('request_'):
			msgid_object = self.proto._REQUEST_MSGID
		elif msg.DESCRIPTOR.name.startswith('response_'):
			msgid_object = self.proto._RESPONSE_MSGID
		else:
			raise IncorrectUse("can't send message %s" % msg.name)

		msgid = msgid_object.values_by_name[msg.DESCRIPTOR.name.upper()]
		body = msg.SerializeToString()
		payload = struct.pack('!II', 4 + len(body), msgid.number) + body

		return payload

	def _recv_msg(self, msgid_enum):
		buf = self._recv_n(4)
		msg_len = struct.unpack('!I', buf)[0]

		buf = self._recv_n(msg_len)
		msgid = struct.unpack('!I', buf[0:4])[0]
		body = buf[4:]

		msgid_enum_value = msgid_enum.values_by_number[msgid]
		msg = getattr(self.proto, msgid_enum_value.name.lower())()
		msg.ParseFromString(body)

		return msg

	def _recv_request_msg(self):
		return self._recv_msg(self.proto._REQUEST_MSGID)

	def _recv_response_msg(self):
		return self._recv_msg(self.proto._RESPONSE_MSGID)

class PBService(common):

	def __init__(self, **kvargs):
		common.__init__(self, kvargs)
		self._connect_timeout = 30

		if 'connect_timeout' in kvargs:
			self._connect_timeout = kvargs['connect_timeout']

	def _connect(self):
		if self._sock is not None:
			return
		self._sock = socket.socket(self._family, socket.SOCK_STREAM)

		self._sock.settimeout(self._connect_timeout)
		err = self._sock.connect_ex(self._addr)
		if err != 0:
			self._clean_close()
			raise IOFailed("can't connect = %d, %s" % (err, os.strerror(err)))
		self._connected = True
		self._sock.settimeout(self._io_timeout)

	@retry_once_on(IOFailed)  # connect, recv or send
	def _pb2_call(self, req):
		self._connect()
		bytes = self._encode_msg(req)
		self._send(bytes)
		return self._recv_response_msg()

	def __getattr__(self, name):
		def call(*a, **kv):
			if len(a):  # arg passed as pb2 object
				req_pb2 = a[0]
			else:  # arg passed as dict
				req_pb2 = getattr(self.proto, 'request_%s' % name)()
				json2pb(req_pb2, kv)
			o = self._pb2_call(req_pb2)
			if o.DESCRIPTOR.name == 'response_generic' and o.error_code != 0:
				self._service_error = o
			else:
				self._service_error = None
			return o
		return call

class PBServer(common):

	class ListeningConnection(asyncore.dispatcher):

		def __init__(self, server):
			asyncore.dispatcher.__init__(self)
			self._server = server
			self.create_socket(server._family, socket.SOCK_STREAM)
			self.set_reuse_addr()
			self.bind(server._addr)
			self.listen(65535)

		def handle_accept(self):
			sock, address = self.accept()
			self._server.client_conns.append(self._server.ClientConnection(self._server, sock, address))

	class ClientConnection(asyncore.dispatcher):

		def __init__(self, server, sock, address):
			asyncore.dispatcher.__init__(self, sock)
			self._server = server
			self.write_buffer = ''
			self.read_buffer = ''
			self.read_msg_id = None
			self.read_msg_len = 0

		def readable(self):
			return len(self.write_buffer) == 0

		def writable(self):
			return len(self.write_buffer) > 0

		def handle_write(self):
			sent = self.send(self.write_buffer)
			self.write_buffer = self.write_buffer[sent:]

		def handle_close(self):
			self._server.client_conns.remove(self)
			self.close()

		def handle_request(self):
			req_msgid = self._server.proto._REQUEST_MSGID.values_by_number[self.read_msg_id]
			req = getattr(self._server.proto, req_msgid.name.lower())()
			req.ParseFromString(self.read_buffer)
			res = getattr(self._server, req_msgid.name.lower())(req)
			self.write_buffer = self._server._encode_msg(res)

		def handle_read(self):
			if self.read_msg_id is None:
				to_recv = 8 - len(self.read_buffer)
				self.read_buffer += self.recv(to_recv)
				if len(self.read_buffer) == 8:
					self.read_msg_len, self.read_msg_id = struct.unpack('!II', self.read_buffer)
					self.read_buffer = ''
					if self.read_msg_len < 4:
						self.close()
						return
					self.read_msg_len -= 4
			if self.read_msg_id is not None:
				if len(self.read_buffer) < self.read_msg_len:
					to_recv = self.read_msg_len - len(self.read_buffer)
					self.read_buffer += self.recv(to_recv)
				if len(self.read_buffer) == self.read_msg_len:
					self.handle_request()
					self.read_buffer = ''
					self.read_msg_id = None
					self.read_msg_len = 0

	def __init__(self, **kvargs):
		common.__init__(self, kvargs)
		self.client_conns = []
		self.listening_conn = self.ListeningConnection(self)

	def generic(self, error_code, error_text=None):
		response = self.proto.response_generic()
		response.error_code = error_code
		if error_text is not None:
			response.error_text = error_text
		return response

	def ok(self):
		return self.generic(0)

	def __getattr__(self, name):
		if name.startswith('error_'):
			errno_name = 'errno_' + name[len("error_"):]
			error_code = -getattr(self.proto, errno_name.upper())
			return lambda error_text: self.generic(error_code, error_text)

	def serve(self):
		asyncore.loop()

if __name__ == '__main__':

	""" client example """

	mm = PBService(host='127.0.0.1', port=11013, proto='meetmaker')
	print mm.user_get(user_id=123)

	""" server example """

	class LaccessServer(PBServer):

		def request_get(self, request):
			response = self.proto.response_users()
			u = response.user.add()
			u.user_id = 115
			return response

		def request_update(self, request):
			return self.error_user_not_exist("user not exist")

		def request_delete(self, request):
			return self.ok()

	s = LaccessServer(host='0.0.0.0', port=11810, proto='laccess')
	s.serve()
