package org.apache.nifi.processors.ext.xml;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
@Tags({"Avro","XML","Product_extend","Sha0w"})
@CapabilityDescription("通过解析输入的Avro文件中的XML字段内" +
        "（xml field）的指定节点（type field），生成新的Avro文件")
public class ProcessProductXML extends AbstractProcessor {
    Logger logger = LoggerFactory.getLogger(ProcessProductXML.class);
    public final static Relationship REL_SUCCESS = new Relationship.Builder()
            .name("sucess")
            .build();

    public final static Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .autoTerminateDefault(true)
            .build();

    public final static PropertyDescriptor XML_FIELD = new PropertyDescriptor.Builder()
            .name("xmlName")
            .required(true)
            .addValidator(Validator.VALID)
            .description("申明avro中需要解析的XML的字段名称")
            .build();

    private static final List<PropertyDescriptor> propertyDescriptors;
    private static final Set<Relationship> relationships;
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    static {
        List<PropertyDescriptor> lpd = new ArrayList<>();
        lpd.add(XML_FIELD);
        propertyDescriptors = Collections.unmodifiableList(lpd);
        Set<Relationship> rs = new HashSet<>();
        rs.add(REL_FAILURE);
        rs.add(REL_SUCCESS);
        relationships = Collections.unmodifiableSet(rs);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final String xmlField = context.getProperty(XML_FIELD).getValue();
        FlowFile flowFile = session.get();
        String type = flowFile.getAttribute("type");

        session.read(flowFile, in -> {
            final DataFileStream<GenericRecord> reader = new DataFileStream<>(in, new GenericDatumReader<GenericRecord>());
            GenericRecord currRecord;
            Schema schema = reader.getSchema();
            List<Schema.Field> fieldList = schema.getFields();
            if (schema.getField(xmlField) == null) {
                throw new AvroRuntimeException("Not a record: "+this);
            }
            //genericRecord -> map
            final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
            final DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
            Set<Map<String, String>> s = new HashSet<>(); //存储所有键值对
            Map<String, String> basic = null;//存储单个键值对
            Set<String> fieldSet = new HashSet<>();//存储所有字段
            while (reader.hasNext()) {
                currRecord = reader.next();//get single gr
                String xml = currRecord.get(xmlField).toString();
                try {
                    Document document = DocumentHelper.parseText(xml);
                    Element rootElem = document.getRootElement(); //GET THE ROOT ELEM
                    basic = processXML(rootElem.element("pub_basic"), fieldSet);
                    basic.putAll(processXML(processExtend(rootElem,"pub_extend","pub_type_id", type),fieldSet));
                } catch (DocumentException e) {
                    e.printStackTrace();
                }
                if (basic == null) {
                    basic = new HashMap<>();
                }
                for (Schema.Field field : fieldList) {
                    if (!Objects.equals(field.name(), xmlField)) {
                        basic.put(field.name(), currRecord.get(field.name()).toString());
                    }
                }
                s.add(basic);
            }
            schema.getFields().forEach(i -> { //remove the xml field, add the sub title
                if (!Objects.equals(i.name(), xmlField)) {
                    fieldSet.add(i.name());
                }
            });
            Schema newSchema = createSchema(fieldSet, type);

            //passing test
            FlowFile ff = session.create(flowFile);
            final GenericRecord rec = new GenericData.Record(newSchema);
            ff = session.write(ff, out -> {
                final DataFileWriter<GenericRecord> dfw = dataFileWriter.create(newSchema, out);
                for (Map<String,String> m : s) {
                    for (String key : m.keySet()) {
                        rec.put(key, m.get(key));
                    }
                    dfw.append(rec);
                }
                dfw.close();
            });
            session.transfer(ff, REL_SUCCESS);
        });
        session.remove(flowFile);
    }

    public static Element processExtend(Element node, String name, String type,String val) {
        List<Element> ele = node.elements(name);
        for (Element element : ele) {
            if (Objects.equals(element.attributeValue(type), val)) {
                return element;
            }
        }
        return null;
    }
    public static Map<String, String> processXML(Element element, Set<String> set) {
        Map<String,String> eleMap = new HashMap<>();
        Iterator<Element> iterator = element.elementIterator();
        Element temp;
        while (iterator.hasNext()) {
            temp = iterator.next();
            set.add(temp.getName());
            eleMap.put(temp.getName(), temp.getStringValue());
        }
        return eleMap;
    }

    public static Schema createSchema(Set<String> set, String type) {
        String tableName = StringUtils.isEmpty(type) ? "NiFi_ProcessProductXML_Record" :  "NiFi_ProcessProductXML_Record_" + type;
        final SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.record(tableName).namespace("any.data").fields();
        for (String s : set) {
            builder.name(s).type().unionOf().nullBuilder().endNull().and().stringType().endUnion().noDefault();
        }
        return builder.endRecord();
    }

}
