
from google.protobuf import descriptor
from google.protobuf import message
from google.protobuf import reflection
from google.protobuf import descriptor_pb2
from google.protobuf.descriptor import FieldDescriptor as dfd

def StripProto(s):
	assert s.endswith('.proto')
	return s[:-len('.proto')]

def ModuleName(s):
	basename = StripProto(s)
	return basename.replace('-', '_').replace('/', '.') + '_pb2'

def GetCppType(t):
	return {
		dfd.TYPE_DOUBLE:    dfd.CPPTYPE_DOUBLE,
		dfd.TYPE_FLOAT:     dfd.CPPTYPE_FLOAT,
		dfd.TYPE_INT64:     dfd.CPPTYPE_INT64,
		dfd.TYPE_UINT64:    dfd.CPPTYPE_UINT64,
		dfd.TYPE_INT32:     dfd.CPPTYPE_INT32,
		dfd.TYPE_FIXED64:   dfd.CPPTYPE_UINT64,
		dfd.TYPE_FIXED32:   dfd.CPPTYPE_UINT32,
		dfd.TYPE_BOOL:      dfd.CPPTYPE_BOOL,
		dfd.TYPE_STRING:    dfd.CPPTYPE_STRING,
		dfd.TYPE_GROUP:     dfd.CPPTYPE_MESSAGE,
		dfd.TYPE_MESSAGE:   dfd.CPPTYPE_MESSAGE,
		dfd.TYPE_BYTES:     dfd.CPPTYPE_STRING,
		dfd.TYPE_UINT32:    dfd.CPPTYPE_UINT32,
		dfd.TYPE_ENUM:      dfd.CPPTYPE_ENUM,
		dfd.TYPE_SFIXED32:  dfd.CPPTYPE_INT32,
		dfd.TYPE_SFIXED64:  dfd.CPPTYPE_INT64,
		dfd.TYPE_SINT32:    dfd.CPPTYPE_INT32,
		dfd.TYPE_SINT64:    dfd.CPPTYPE_INT64,
	}[t]

class FDPLinker:

	def __init__(self):
		self._names = {} # messages and enums fq names
		self._containing_type = {} # parents by id
		self._message_of_field = {}
		self._enum_of_field = {}
		self._extension_scope = {}
		self._is_extension = {}
		self._obj2pb2 = {}

	def add(self, fdp):

		def fqname(name_components):
			fq = "." + ".".join(name_components)
			if len(fdp.package):
				fq = "." + fdp.package + fq
			return fq

		def walk1(name, parent, messages, extensions, enums):
			for enum in enums:
				fq = fqname(name + [enum.name])
				assert fq not in self._names
				self._names[fq] = enum
				self._containing_type[id(enum)] = parent
			for msg in messages:
				fq = fqname(name + [msg.name])
				assert fq not in self._names
				self._names[fq] = msg
				self._containing_type[id(msg)] = parent
				for field in msg.field:
					self._containing_type[id(field)] = msg
					self._is_extension[id(field)] = False
				walk1(name + [msg.name], msg, msg.nested_type, msg.extension, msg.enum_type)
			for ext in extensions:
				self._containing_type[id(ext)] = parent
				self._is_extension[id(ext)] = True

		walk1([], None, fdp.message_type, fdp.extension, fdp.enum_type)

		def walk2(name, messages, extensions):
			def resolve_field(name, field, link_name):
				if link_name.startswith('.'):
					return link_name
				else:
					# search with cpp scoping rules, as described in descriptor.proto
					search = list(name)
					while fqname(search + [link_name]) not in self._names:
						if len(search) == 0:
							raise Exception("link_name %s not found for field %s" % (link_name, fqname(name + [field.name])))
						del search[-1]
					return fqname(search + [link_name])

			def link_msgs_and_enums(name, field):
				if len(field.type_name):
					if field.type not in (dfd.TYPE_MESSAGE, dfd.TYPE_ENUM):
						raise Exception("field %s has type %d however its type not message nor enum" % (fqname(name + [field.name]), field.type))
					fq = resolve_field(name, field, field.type_name)
					if field.type == dfd.TYPE_MESSAGE:
						self._message_of_field[id(field)] = self._names[fq]
					else:
						self._enum_of_field[id(field)] = self._names[fq]

			for msg in messages:
				for field in msg.field:
					link_msgs_and_enums(name, field)
				walk2(name + [msg.name], msg.nested_type, msg.extension)

			for ext in extensions:
				if len(ext.extendee) == 0:
					raise Exception("field %s pretends to be extension however has empty extendee" % fqname(name + [ext.name]))
				fq = resolve_field(name, ext, ext.extendee)
#				print "binding", fqname(name + [ext.name]), "to", fq, "(extendee %s)" % ext.extendee
				self._extension_scope[id(ext)] = self._names[fq]
				assert self._names[fq].DESCRIPTOR.name == 'DescriptorProto' # there might be enums as well
				link_msgs_and_enums(name, ext)

		walk2([], fdp.message_type, fdp.extension)

	def containing_type(self, item):
		return self._containing_type[id(item)]

	def message_of_field(self, field):
		return self._message_of_field.get(id(field), None)

	def enum_of_field(self, field):
		return self._enum_of_field.get(id(field), None)

	def extension_scope(self, ext):
		return self._extension_scope[id(ext)]

	def is_extension(self, field):
		return self._is_extension[id(field)]

	def add_pb2(self, obj, pb2):
		self._obj2pb2[id(obj)] = pb2

	def pb2(self, obj):
		return self._obj2pb2.get(id(obj), None)

class DynFDP:

	"""
		no checks that fdp is valid protoc product
		no groups
		no services
	"""

	def __init__(self, descriptor_bin, fdp=None, linker=None):

		# can only pass exactly one of them
		assert (descriptor_bin is None) != (fdp is None)

		if descriptor_bin is None:
			self.descriptor_bin = fdp.SerializeToString()
			self.fdp = fdp
		else:
			self.descriptor_bin = descriptor_bin
			self.fdp = descriptor_pb2.FileDescriptorProto()
			self.fdp.ParseFromString(descriptor_bin)

		if linker is None:
			self.linker = FDPLinker()
		else:
			self.linker = linker

		self.linker.add(self.fdp)

		self.module = type(__import__('sys'))(ModuleName(str(self.fdp.name)))
		setattr(self.module, 'descriptor', descriptor)
		setattr(self.module, 'message', message)
		setattr(self.module, 'reflection', reflection)
		setattr(self.module, 'descriptor_pb2', descriptor_pb2)

		# these names copied from python_generator.cc
		# all functions of these methods are preserved as much as possible
		self.PrintFileDescriptor()
		self.PrintTopLevelEnums()
		self.PrintTopLevelExtensions()
		self.PrintAllNestedEnumsInFile()
		self.PrintMessageDescriptors()
		self.FixForeignFieldsInDescriptors()
		self.PrintMessages()
		self.FixForeignFieldsInExtensions()

	def NamePrefixedWithNestedTypes(self, item, separator):
		name = item.name
		while True:
			parent = self.linker.containing_type(item)
			if parent is None:
				break
			item = parent
			name = item.name + separator + name
		return str(name)

	def ModuleLevelDescriptorName(self, item):
		name = self.NamePrefixedWithNestedTypes(item, '_')
		# fixme: for foreign items we must add more name components
		return '_' + name.upper()

	def ModuleLevelMessageName(self, item):
		name = self.NamePrefixedWithNestedTypes(item, '.')
		# fixme: for foreign items we must add more name components
		return name

	def PrintFileDescriptor(self):
		setattr(self.module, 'DESCRIPTOR', descriptor.FileDescriptor(
			name=str(self.fdp.name),
			package=str(self.fdp.package),
			serialized_pb=self.descriptor_bin))

	def FullName(self, item):
		name = self.NamePrefixedWithNestedTypes(item, '.')
		if len(self.fdp.package):
			name = self.fdp.package + '.' + name
		return str(name)

	def SerializedPbInterval(self, item):
		s = item.SerializeToString()
		pos = self.descriptor_bin.index(s)
		return pos, pos + len(s)

	def OptionsValue(self, class_name, options):
		serialized = options.SerializeToString()
		if len(serialized) == 0:
			return None
		return descriptor._ParseOptions(getattr(descriptor_pb2, class_name)(), serialized)

	def EnumValueDescriptor(self, index, item):
		return descriptor.EnumValueDescriptor(
			name=str(item.name),
			index=index,
			number=item.number,
			options=self.OptionsValue("EnumValueOptions", item.options),
			type=None)

	def EnumDescriptor(self, item):
		ss, se = self.SerializedPbInterval(item)
		e = descriptor.EnumDescriptor(
			name=str(item.name),
			full_name=self.FullName(item),
			filename=None,
			file=self.module.DESCRIPTOR,
			values=[ self.EnumValueDescriptor(index, value) for index, value in enumerate(item.value) ],
			containing_type=None,
			options=self.OptionsValue("EnumOptions", item.options),
			serialized_start=ss,
			serialized_end=se
		)
		self.linker.add_pb2(item, e)
		return e

	def PrintTopLevelEnums(self):
		for enum_type in self.fdp.enum_type:
			setattr(self.module, self.ModuleLevelDescriptorName(enum_type), self.EnumDescriptor(enum_type))
			for ev in enum_type.value:
				setattr(self.module, ev.name, ev.number)

	def PrintTopLevelExtensions(self):
		for index, field in enumerate(self.fdp.extension):
			name = str(field.name).upper() + "_FIELD_NUMBER"
			setattr(self.module, name, field.number)
			setattr(self.module, field.name, self.FieldDescriptor(index, field, True))

	def PrintAllNestedEnumsInFile(self):
		def PrintNestedEnums(msg):
			for message_type in msg.nested_type:
				PrintNestedEnums(message_type)
			for enum_type in msg.enum_type:
				setattr(self.module, self.ModuleLevelDescriptorName(enum_type), self.EnumDescriptor(enum_type))
		for message_type in self.fdp.message_type:
			PrintNestedEnums(message_type)

	def StringifyDefaultValue(self, field):
		if field.label == dfd.LABEL_REPEATED:
			return []

		if field.type == dfd.TYPE_BYTES:
			return str(field.default_value).decode('string_escape')

		t = GetCppType(field.type)

		if t in (dfd.CPPTYPE_INT32, dfd.CPPTYPE_UINT32, dfd.CPPTYPE_INT64, dfd.CPPTYPE_UINT64):
			if field.default_value == "":
				return 0
			else:
				return int(field.default_value)
		elif t in (dfd.CPPTYPE_DOUBLE, dfd.CPPTYPE_FLOAT):
			# Original protobuf compiler incorrectly set some of default values for floats and doubles as ints, so we have to mimic that
			if field.default_value == "":
				return 0
			else:
				try:
					return int(field.default_value)
				except:
					pass
				return float(field.default_value)
		elif t == dfd.CPPTYPE_BOOL:
			if field.default_value == "":
				return False
			elif field.default_value.lower() == 'false':
				return False
			elif field.default_value.lower() == 'true':
				return True
			else:
				raise Exception("unrecognized bool value " + field.default_value)
		elif t == dfd.CPPTYPE_ENUM:
			enum = self.linker.enum_of_field(field)
			if len(field.default_value):
				return filter(lambda ev: ev.name == field.default_value, enum.value)[0].number
			else:
#				print field.name
				return enum.value[0].number
		elif t == dfd.CPPTYPE_STRING:
			return field.default_value
		elif t == dfd.CPPTYPE_MESSAGE:
			return None

		raise Exception("Unreachable")

	def FieldDescriptor(self, index, field, is_extension):
		return descriptor.FieldDescriptor(
			name=str(field.name),
			full_name=self.FullName(field),
			index=index,
			number=field.number,
			type=field.type,
			cpp_type=GetCppType(field.type),
			label=field.label,
			has_default_value=field.HasField('default_value'),
			default_value=self.StringifyDefaultValue(field),
			message_type=None,
			enum_type=None,
			containing_type=None,
			is_extension=is_extension,
			extension_scope=None,
			options=self.OptionsValue("FieldOptions", field.options)
		)

	def PrintFieldDescriptorsInDescriptor(self, is_extension, lst):
		return [self.FieldDescriptor(index, field, is_extension) for index, field in enumerate(lst)]

	def MessageDescriptor(self, msg):
		ss, se = self.SerializedPbInterval(msg)
		m = descriptor.Descriptor(
			name=str(msg.name),
			full_name=self.FullName(msg),
			filename=None,
			file=self.module.DESCRIPTOR,
			containing_type=None,
			fields=self.PrintFieldDescriptorsInDescriptor(False, msg.field),
			extensions=self.PrintFieldDescriptorsInDescriptor(True, msg.extension),
			nested_types=[ getattr(self.module, self.ModuleLevelDescriptorName(message_type))
				for message_type in msg.nested_type ],
			enum_types=[ getattr(self.module, self.ModuleLevelDescriptorName(enum_type))
				for enum_type in msg.enum_type ],
			options=self.OptionsValue("MessageOptions", msg.options),
			is_extendable=(len(msg.extension_range) > 0),
			extension_ranges=[ (er.start, er.end) for er in msg.extension_range ],
			serialized_start=ss,
			serialized_end=se)
		self.linker.add_pb2(msg, m)
		return m

	def PrintMessageDescriptors(self):
		def PrintDescriptor(msg):
			for message_type in msg.nested_type:
				PrintDescriptor(message_type)
			setattr(self.module, self.ModuleLevelDescriptorName(msg), self.MessageDescriptor(msg))
		for message_type in self.fdp.message_type:
			PrintDescriptor(message_type)

	def FieldReferencingExpression(self, msg, field, dict_name):
		# xxx: check if field.file is from self.fdp
		if msg is None:
			return getattr(self.module, field.name)
#		print dict_name, self.ModuleLevelDescriptorName(msg)
		m_obj = getattr(self.module, self.ModuleLevelDescriptorName(msg))
		d_obj = getattr(m_obj, dict_name)
#		print "FieldReferencingExpression", msg, field.name, dict_name, d_obj
		return d_obj.get(field.name, None)

	def FixForeignFieldsInDescriptor(self, msg, parent):
		for message_type in msg.nested_type:
			self.FixForeignFieldsInDescriptor(message_type, msg)
		for field in msg.field:
			self.FixForeignFieldsInField(msg, field, "fields_by_name");

		self.FixContainingTypeInDescriptor(msg, parent)

		for enum_type in msg.enum_type:
			self.FixContainingTypeInDescriptor(enum_type, msg)

	def FixForeignFieldsInField(self, msg, field, dict_name):
		obj = self.FieldReferencingExpression(msg, field, dict_name)
		m = self.linker.message_of_field(field)
		if m is not None:
			m_obj = self.linker.pb2(m)
			obj.message_type = m_obj
		e = self.linker.enum_of_field(field)
		if e is not None:
			e_obj = self.linker.pb2(e)
			obj.enum_type = e_obj

	def FixForeignFieldsInDescriptors(self):
		for message_type in self.fdp.message_type:
			self.FixForeignFieldsInDescriptor(message_type, None)
		for message_type in self.fdp.message_type:
			self.AddMessageToFileDescriptor(message_type)

	def AddMessageToFileDescriptor(self, msg):
		m = getattr(self.module, self.ModuleLevelDescriptorName(msg))
		self.module.DESCRIPTOR.message_types_by_name[msg.name] = m

	def FixContainingTypeInDescriptor(self, item, parent):
		if parent is not None:
			parent = getattr(self.module, self.ModuleLevelDescriptorName(parent))
			item = getattr(self.module, self.ModuleLevelDescriptorName(item))
			item.containing_type = parent

	def PrintMessages(self):
		# Ahhh. Recursive metaclasses.
		def GatherAttrs(messages):
			attrs = {}
			for msg in messages:
				msg_descriptor = getattr(self.module, self.ModuleLevelDescriptorName(msg))
				msg_attrs = {
					'DESCRIPTOR': msg_descriptor,
					'__metaclass__': reflection.GeneratedProtocolMessageType, # not needed, but it exists in original _pb2 generated code
				}
				msg_attrs.update(GatherAttrs(msg.nested_type))
				cls = reflection.GeneratedProtocolMessageType(str(msg.name), (message.Message,), msg_attrs)
				attrs[msg.name] = cls
			return attrs

		for k, v in GatherAttrs(self.fdp.message_type).items():
			setattr(self.module, k, v)

	def FixForeignFieldsInExtensions(self):
		def FixForeignFieldsInExtension(ext):
			assert self.linker.is_extension(ext)
			self.FixForeignFieldsInField(self.linker.containing_type(ext), ext, "extensions_by_name")
			field = self.FieldReferencingExpression(self.linker.containing_type(ext), ext, "extensions_by_name")
			extended_message_class = getattr(self.module, self.ModuleLevelMessageName(self.linker.extension_scope(ext)))
#			print extended_message_class, field
			extended_message_class.RegisterExtension(field)
		def FixForeignFieldsInNestedExtensions(msg):
			for m in msg.nested_type:
				FixForeignFieldsInNestedExtensions(m)
			for ext in msg.extension:
				FixForeignFieldsInExtension(ext)
		for ext in self.fdp.extension:
			FixForeignFieldsInExtension(ext)
		for msg in self.fdp.message_type:
			FixForeignFieldsInNestedExtensions(msg)

class DynFDS:

	def __init__(self, descriptor_bin):

		self.descriptor_bin = descriptor_bin

		self.fds = descriptor_pb2.FileDescriptorSet()
		self.fds.ParseFromString(descriptor_bin)

		self.linker = FDPLinker()

		self.fdp = {}

		files = {}

		for file in self.fds.file:
			files[str(file.name)] = file

		def add(name, trace):
			if name in self.fdp:
				return
			if name in trace:
				raise Exception("cyclic dependencies detected: %s -> %s" % (name, trace))
			file = files[name]
			for dep in file.dependency:
				if dep not in files:
					raise Exception("missing dependency: %s -> %s" % (name, dep))
				add(str(dep), trace + [name])
			self.fdp[name] = DynFDP(None, file, self.linker)

		for name in files.keys():
			add(name, [])
