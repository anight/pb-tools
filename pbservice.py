
# (c) 2010, Andrei Nigmatulin

import os, socket, struct
from protobuf_json import json2pb

class PBService:

	def __init__(self, **kvargs):
		self.host = kvargs['host']
		self.port = int(kvargs['port'])
		self.proto = __import__(kvargs['proto'] + '_pb2')
		self.connected = False

	def _connect(self):
		if self.connected:
			return
		self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		err = self.sock.connect_ex((self.host, self.port))
		if err != 0:
			raise Exception("can't connect = %d, %s" % (err, os.strerror(err)))
		self.connected = True

	def _recv_n(self, n):
		buf = ''

		while (len(buf) < n):
			chunk = self.sock.recv(n - len(buf))
			if chunk is None:
				break
			buf = buf + chunk

		if len(buf) != n:
			self.sock.close()
			self.connected = False
			raise Exception("Truncated response: received %d bytes from %d expected" % (len(buf), n))

		return buf

	def _pb2_call(self, req):

		""" Hides service i/o, message ids and other binary protocol stuff """

		self._connect()

		req_msgid = self.proto._REQUEST_MSGID.values_by_name[req.DESCRIPTOR.name.upper()].number
		bin_data = req.SerializeToString()
		payload = struct.pack('!II', len(bin_data) + 4, req_msgid) + bin_data

		self.sock.sendall(payload)

		buf = self._recv_n(4)
		res_len = struct.unpack('!I', buf)[0]

		buf = self._recv_n(res_len)
		res_msgid = struct.unpack('!I', buf[0:4])[0]
		res_body = buf[4:]

		res_name = self.proto._RESPONSE_MSGID.values_by_number[res_msgid].name
		res = getattr(self.proto, res_name.lower())()
		res.ParseFromString(res_body)

		return res

	def __getattr__(self, name):
		def call(*a, **kv):
			if len(a): # arg passed as pb2 object
				req_pb2 = a[0]
			else: # arg passed as dict
				req_pb2 = getattr(self.proto, 'request_%s' % name)()
				json2pb(req_pb2, kv)
			return self._pb2_call(req_pb2)
		return call

if __name__ == '__main__':

	""" Usage example """

	import sys
	sys.path.insert(0, '../geoborder/proto')
	geoborder = PBService(host='127.0.0.1', port=11853, proto='geoborder')
	print geoborder.locate(lon=-0.50880, lat=51.67577)
