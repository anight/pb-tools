#! /usr/bin/env python
'''
Provide serialization and de-serialization of Google's protobuf Messages into/from JSON format.
'''

# groups are deprecated and not supported;
# Note that preservation of unknown fields is currently not available for Python (c) google docs
# extensions is not supported from 0.0.5 (due to gpb2.3 changes)

__version__='0.0.5'
__author__='Paul Dovbush <dpp@dpp.su>'


import json	# py2.6+ TODO: add support for other JSON serialization modules
from google.protobuf.descriptor import FieldDescriptor as FD


class ParseError(Exception): pass


def json2pb(pb, js):
	''' convert JSON string to google.protobuf.descriptor instance '''

	for field in pb.DESCRIPTOR.fields:
		if field.name not in js:
			continue
		if field.type == FD.TYPE_MESSAGE:
			pass
		elif field.type == FD.TYPE_ENUM:
			pass
		elif field.type in _js2ftype:
			ftype = _js2ftype[field.type]
		else:
			raise ParseError("Field %s.%s of type '%d' is not supported" % (pb.__class__.__name__, field.name, field.type, ))
		value = js[field.name]
		if field.label == FD.LABEL_REPEATED:
			pb_value = getattr(pb, field.name, None)
			for v in value:
				if field.type == FD.TYPE_MESSAGE:
					json2pb(pb_value.add(), v)
				elif field.type == FD.TYPE_ENUM:
					pb_value.append(enum_value_as_int(field, value))
				else:
					pb_value.append(ftype(v))
		else:
			if field.type == FD.TYPE_MESSAGE:
				json2pb(getattr(pb, field.name, None), value)
			elif field.type == FD.TYPE_ENUM:
				setattr(pb, field.name, enum_value_as_int(field, value))
			else:
				setattr(pb, field.name, ftype(value))
	return pb


def enum_value_as_int(field, value):
	d = field.enum_type.values_by_name

	if type(value) in (str, unicode):
		if value in d:
			return d[value].number
		else:
			raise ParseError("Field %s unknown enum value '%s'" % (field.full_name, value))
	else:
		return value


def enum_value_as_str(field, value):
	d = field.enum_type.values_by_number

	if type(value) in (int, long):
		if value in d:
			return d[value].name
		else:
			raise ParseError("Field %s unknown enum value '%s'" % (field.full_name, value))
	else:
		return value


def pb2json(pb):
	''' convert google.protobuf.descriptor instance to JSON string '''
	js = {}
	# fields = pb.DESCRIPTOR.fields #all fields
	fields = pb.ListFields()	#only filled (including extensions)

	def field_get_value(field, value):
		if field.type == FD.TYPE_MESSAGE:
			return pb2json(value)
		elif field.type == FD.TYPE_ENUM:
			return enum_value_as_str(field, value)
		elif field.type in _ftype2js:
			return _ftype2js[field.type](value)
		else:
			raise ParseError("Field %s.%s of type '%d' is not supported" % (pb.__class__.__name__, field.name, field.type))

	for field,value in fields:
		if field.label == FD.LABEL_REPEATED:
			js_value = []
			for v in value:
				js_value.append(field_get_value(field, v))
		else:
			js_value = field_get_value(field, value)

		js[field.name] = js_value
	return js


_ftype2js = {
	FD.TYPE_DOUBLE: float,
	FD.TYPE_FLOAT: float,
	FD.TYPE_INT64: int,
	FD.TYPE_UINT64: int,
	FD.TYPE_INT32: int,
	FD.TYPE_FIXED64: int,
	FD.TYPE_FIXED32: int,
	FD.TYPE_BOOL: bool,
	FD.TYPE_STRING: str,
	#FD.TYPE_MESSAGE: pb2json, #handled specially
	FD.TYPE_BYTES: lambda x: x,
	FD.TYPE_UINT32: int,
	# FD.TYPE_ENUM: int,
	FD.TYPE_SFIXED32: int,
	FD.TYPE_SFIXED64: int,
	FD.TYPE_SINT32: int,
	FD.TYPE_SINT64: int,
}

_js2ftype = {
	FD.TYPE_DOUBLE: float,
	FD.TYPE_FLOAT: float,
	FD.TYPE_INT64: int,
	FD.TYPE_UINT64: int,
	FD.TYPE_INT32: int,
	FD.TYPE_FIXED64: int,
	FD.TYPE_FIXED32: int,
	FD.TYPE_BOOL: bool,
	FD.TYPE_STRING: str,
	# FD.TYPE_MESSAGE: json2pb,	#handled specially
	FD.TYPE_BYTES: lambda x: x,
	FD.TYPE_UINT32: int,
	# FD.TYPE_ENUM: int, # #handled specially
	FD.TYPE_SFIXED32: int,
	FD.TYPE_SFIXED64: int,
	FD.TYPE_SINT32: int,
	FD.TYPE_SINT64: int,
}

