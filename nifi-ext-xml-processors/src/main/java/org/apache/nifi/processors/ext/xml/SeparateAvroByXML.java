package org.apache.nifi.processors.ext.xml;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
// {id+xml -> [id + basic xml + extend xml + type](每个attr一个ff)}
@Tags({"Avro","XML","Split","sha0w"})
@CapabilityDescription("从其他地方得到一个AVRO流，通过这个处理器，你可以指定这个Avro中的一个XML字段，该处理器会解析这个XML" +
        "字段，并在该XML中寻找一个你输入的代表路径的XML字段名称，并通过这个字段名称将输入的Avro文件分成不同的几份 老版本")
@WritesAttribute(attribute = "type", description = "This processor adds user-defined attributes if the <Destination> property is set to flowfile-attribute.")
public class SeparateAvroByXML extends AbstractProcessor{
    public static final String UTF8_BOM = "\uFEFF";     //http://www.rgagnon.com/javadetails/java-handle-utf8-file-with-bom.html
    private static Logger logger = LoggerFactory.getLogger(SeparateAvroByXML.class);
    public static final PropertyDescriptor XML_DECODE_FIELD = new PropertyDescriptor.Builder()
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .description("输入这个avro中你需要解析的XML字段名称")
            .name("xml decode field")
            .build();

    public static final PropertyDescriptor XML_TYPE_FIELD = new PropertyDescriptor.Builder()
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .description("输入这个字段中代表类型的字段，要求使用XPATH语法")
            .name("xml type field")
            .build();
    public static final PropertyDescriptor XML_TYPE_FIELD_NAME = new PropertyDescriptor.Builder()
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("输入这个XML字段中代表类型的字段名称")
            .name("xml type field name")
            .build();
    public static final PropertyDescriptor XML_UNIQUE_FIELD = new PropertyDescriptor.Builder()
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("这个字段代表该XML中的类型特定的XML，要求使用XPATH语法，会在之后的解析中加入type类型")
            .name("xml unique type")
            .build();

    public static final PropertyDescriptor XML_COMMON_FIELD = new PropertyDescriptor.Builder()
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("这个字段代表该XML中的通用的XML，要求使用XPATH语法")
            .name("xml common type")
            .build();
    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully created FlowFile from SQL query result set.")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("SQL query execution failed. Incoming FlowFile will be penalized and routed to this relationship")
            .build();

    private static final List<PropertyDescriptor> propertyDescriptors;
    private static final Set<Relationship> relationships;
    static {
        List<PropertyDescriptor> _props = new ArrayList<>();
        _props.add(XML_DECODE_FIELD);
        _props.add(XML_TYPE_FIELD);
        _props.add(XML_COMMON_FIELD);
        _props.add(XML_UNIQUE_FIELD);
        _props.add(XML_TYPE_FIELD_NAME);
        propertyDescriptors = Collections.unmodifiableList(_props);
        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_FAILURE);
        _relationships.add(REL_SUCCESS);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
       FlowFile flowFile = session.get();
       if (flowFile == null) {
           return;
       }
       final String xmlUniqueField = context.getProperty(XML_UNIQUE_FIELD).getValue();
       final String xmlCommonField = context.getProperty(XML_COMMON_FIELD).getValue();
       final String xmlTypeFieldName = context.getProperty(XML_TYPE_FIELD_NAME).getValue();
       //build new avro schema
       Set<String> newSchema = new HashSet<>();
       newSchema.add("xmlCommonField");
       newSchema.add("xmlUniqueField");
       newSchema.add("type");
       final String xmlField = context.getProperty(XML_DECODE_FIELD).getValue();
       final String xmlTypeField = context.getProperty(XML_TYPE_FIELD).getValue();
       final List<FlowFile> ffList = new ArrayList<>();
       final Map<String, ConcurrentLinkedQueue<GenericRecord>> grMap = new ConcurrentHashMap<>();
       try {
           session.read(flowFile, in -> {
               final DataFileStream<GenericRecord> reader = new DataFileStream<>(in, new GenericDatumReader<GenericRecord>());
               GenericRecord currRecord;

               Schema schema = reader.getSchema();

               if (schema.getField(xmlField) == null) {
                   throw new AvroRuntimeException("Not a record: "+this);
               }

               while (reader.hasNext()) {
                   currRecord = reader.next();
                   String xml = currRecord.get(xmlField).toString();
                   String key = getXmlValue(xml,xmlTypeField);
                   if (grMap.get(key) != null) {
                       grMap.get(key).add(currRecord);
                   } else {
                      grMap.put(key,new ConcurrentLinkedQueue<>());
                      grMap.get(key).add(currRecord);
                   }
               }
               final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
               final DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
               for (String key : grMap.keySet()) {
                   ConcurrentLinkedQueue<GenericRecord> gr = grMap.get(key);
                   Schema newBuildSchema = mkNewSchema(schema,newSchema,null,xmlField);
                   FlowFile ff = session.create(flowFile);
                   final GenericRecord rec = new GenericData.Record(newBuildSchema);
                   ff = session.write(ff, out -> {
                       final DataFileWriter<GenericRecord> dfw = dataFileWriter.create(newBuildSchema, out);
                       for (GenericRecord genericRecord : gr) {
                           String xml = genericRecord.get(xmlField).toString();
                           rec.put("xmlCommonField", getXml(xml, xmlCommonField));
                           rec.put("xmlUniqueField", getXml(xml, xmlUniqueField + "[@" + xmlTypeFieldName+ "=\"" + key + "\"]"));
                           rec.put("type", key);
                           for (Schema.Field field : schema.getFields()) {
                               if (!Objects.equals(field.name(), xmlField)) {
                                   rec.put(field.name(),genericRecord.get(field.name()));
                               }
                           }
                           dfw.append(rec);
                       }
                       dfw.close();
                   });
                   ff = session.putAttribute(ff, "type", key); // 一定要赋值回一个flowFile变量：
                   ffList.add(ff);
               }
           });
           session.transfer(ffList,REL_SUCCESS);
           session.remove(flowFile);
       } catch (Exception e) {
           session.remove(ffList);
           session.transfer(flowFile, REL_FAILURE);
           e.printStackTrace();
       }
    }


    //using xpath test pass
    private static String getXmlValue(String xml, String path) {
        Document doc = null;
        try {
            doc = DocumentHelper.parseText(xml);
        } catch (DocumentException e) {
            try {
                //0x.
                String af = CheckUnicodeString(xml);
                //&#.
                doc = DocumentHelper.parseText(af.replaceAll("&#.", " ").replaceAll(UTF8_BOM," "));
            } catch (DocumentException e1) {
                logger.error(e1.getMessage());
            }
            logger.error(e.getMessage());
        }
        assert doc != null;
        Element temp = doc.getRootElement();
        return temp.selectSingleNode(path).getText();
    }
    //using xpath test pass
    private static String getXml(String xml, String path) {
        Document doc = null;
        try {
            doc = DocumentHelper.parseText(xml);
        } catch (DocumentException e) {
            try {
                String af = CheckUnicodeString(xml);
                doc = DocumentHelper.parseText(af.replaceAll("&#.", " ").replaceAll(UTF8_BOM," "));
            } catch (DocumentException e1) {
//                e1.printStackTrace();
            }
//            e.printStackTrace();
        }
        assert doc != null;
        Element temp = doc.getRootElement();
        return temp.selectSingleNode(path).asXML();
    }

    public static Schema createSchema(Set<String> set, String type) {
        String tableName = StringUtils.isEmpty(type) ? "NiFi_SeparateAvroByXML_Record" :  "NiFi_SeparateAvroByXML_Record_" + type;
        final SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.record(tableName).namespace("any.data").fields();
        for (String s : set) {
            builder.name(s).type().unionOf().nullBuilder().endNull().and().stringType().endUnion().noDefault();
        }
        return builder.endRecord();
    }

    public static Schema mkNewSchema(Schema old, Set<String> set, String type, String notuse) {
        Set<String> old_field = new HashSet<>();
        for (Schema.Field field : old.getFields()) {
            if (!Objects.equals(field.name(), notuse)) {
                old_field.add(field.name());
            }
        }
        old_field.addAll(set);
        return createSchema(old_field, type);
    }
    public static String CheckUnicodeString(String value) {
        char[] valueArr = value.toCharArray();
        for (int i = 0; i < value.length(); ++i) {
            if (valueArr[i] > 0xFFFD) {
                valueArr[i] = ' ';
            } else if (valueArr[i] < 0x20) {
                valueArr[i]= ' ';
            }
        }
        return String.copyValueOf(valueArr);
    }
}
