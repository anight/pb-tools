#! /usr/bin/env python

PROTOC = "/local/protobuf/bin/protoc"

import os,sys,socket,struct
import json
from protobuf_json import json2pb, pb2json

def add_import_path(*args):
    start_path = os.path.join(os.path.dirname('.'))
    for arg in args:
        path = os.path.join(start_path, arg)
        if os.path.exists(path):
            sys.path.insert(0, os.path.join(start_path, arg))
        else:
            raise ImportError("Path '%s' does not exists [base=%s, rel=%s]" % (path, start_path, arg))

def pb2_compile_import(proto_file):
    tmp_path = '/tmp' #os.path.expandvars('%TMP%')
    proto_file = os.path.abspath(proto_file)
    cmd = "%s --python_out=%s --proto_path=%s %s" % (PROTOC, tmp_path, os.path.dirname(proto_file), proto_file, )
    er = os.system(cmd)
    if er != 0:
        print("Command %s exit code = %d" % (cmd, er, ))
        return

    add_import_path(tmp_path)
    base_name = os.path.basename(proto_file)
    module_name = base_name[:base_name.rfind('.')]+'_pb2'

    pb2 = __import__(module_name)
    os.unlink(os.path.join(tmp_path, module_name+'.py'))
    return pb2

def pb2_import(proto_file):
    if os.path.sep in proto_file:
        path = os.path.dirname(proto_file)
        add_import_path(path)
        module_name = os.path.basename(proto_file)
        module_name = module_name[:module_name.rfind('.')]
    else:
        module_name = proto_file
    pb2 = __import__(module_name)
    return pb2


def _recv_n(sock, n):
    buf = ''
    while (len(buf) < n):
        chunk = sock.recv(n - len(buf))
        if not chunk:
            sock.close()
            raise Exception('recv return None')
        buf = buf + chunk
        return buf

def request(ip, port, req_id, bin_data):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    err = sock.connect_ex((ip, port))
    if err != 0:
        raise Exception("can't connect = %d, %s" % (err, os.strerror(err)))

    data = "%s%s%s" % (struct.pack('!I', len(bin_data) + 4), struct.pack('!I', req_id), bin_data)
    sock.sendall(data)

    buf = _recv_n(sock,4)
    (res_len,) = struct.unpack('!I',buf)

    res = _recv_n(sock,res_len)
    if len(res) != res_len:
        sock.close()
        raise Exception("Received %d bytes, but %d bytes where announced" % (len(res), res_len))
    (res_type,) = struct.unpack('!I',res[0:4])
    res_body = res[4:]

    return res_type, res_body



def main(argv):
    if len(argv) != 6:
        print('Usage: ./x ip port gpb_file.proto request_msgid request_name request_body_in_json')
        return
    ip, port, proto_file, req_msgid, req_name, req_body = argv
    port = int(port)

    if '.proto' == proto_file[proto_file.rfind('.'):]:
        pb2 = pb2_compile_import(proto_file)
    else:
        pb2 = pb2_import(proto_file)

    req_id = None
    try:
        req_id = int(req_msgid)
    except ValueError:
        req_msgid = req_msgid.lower()
        for item in pb2._REQUEST_MSGID.values:
            name = item.name.lower()
            if name == req_msgid:
                req_id = item.number
                break
    if req_id is None:
        print('unknown request id `%s`' % req_msgid)
        return

    req_ctor = getattr(pb2, req_name)

    try:
        req_json = json.loads(req_body)
    except (Exception, exc):
        print('cant parse JSON\n%s' % exc)
        return 

    req = json2pb(req_ctor(), req_json)

    #print req_id, req.SerializeToString().encode('string-escape')
    res_type, res_body = request(ip, port, req_id, req.SerializeToString())
    #print res_type, res_body.encode('string-escape')

    #if res_type not in rout:
    #    print 'unknown response %d' % res_type

    for item in pb2._RESPONSE_MSGID.values:
        if res_type == item.number:
            name = item.name.lower()
            res_name, res_ctor = name, getattr(pb2, name)

    #res_name, res_ctor = rout[res_type]
    res = res_ctor()
    res.ParseFromString(res_body)
    print("RESPONSE: %s\n%s\n" % (res_name, json.dumps(pb2json(res), indent=4)))

if __name__ == '__main__':
    main(sys.argv[1:])
