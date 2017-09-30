package org.apache.nifi.processors.ext.xml;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
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
@Tags({"Avro","XML","Split","sha0w"})
@CapabilityDescription("从其他地方得到一个AVRO流，通过这个处理器，你可以指定这个Avro中的一个XML字段，该处理器会解析这个XML" +
        "字段，并在该XML中寻找一个你输入的代表路径的XML字段名称，并通过这个字段名称将输入的Avro文件分成不同的几份")
public class SeparateAvroByXML extends AbstractProcessor{
    Logger logger = LoggerFactory.getLogger(SeparateAvroByXML.class);
    public static final PropertyDescriptor XML_DECODE_FIELD = new PropertyDescriptor.Builder()
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .description("输入这个avro中你需要解析的XML字段")
            .name("xml decode field")
            .build();

    public static final PropertyDescriptor XML_TYPE_FIELD = new PropertyDescriptor.Builder()
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .description("输入这个字段中代表类型的字段")
            .name("xml type field")
            .build();

    public static final PropertyDescriptor AVRO_ID_FIELD = new PropertyDescriptor.Builder()
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("extend表中需要保留的字段")
            .name("keep id")
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
        _props.add(AVRO_ID_FIELD);
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
                   String key = null;
                   try {
                       key = getXml(xml,xmlTypeField);
                   } catch (DocumentException e) {
                       e.printStackTrace();
                   }

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
                   FlowFile ff = session.create(flowFile);
                   ff = session.write(ff, out -> {
                       final DataFileWriter<GenericRecord> dfw = dataFileWriter.create(schema, out);
                       for (GenericRecord genericRecord : gr) {
                           dfw.append(genericRecord);
                       }
                       dfw.close();
                   });
                   logger.debug(key);
                   ff = session.putAttribute(ff, "type", key); // 一定要赋值回一个flowFile变量：
                   ffList.add(ff);
               }
           });

       } catch (Exception e) {
           e.printStackTrace();
           session.remove(ffList);
           session.transfer(flowFile,REL_FAILURE);
       }
       session.remove(flowFile);
       session.transfer(ffList,REL_SUCCESS);
    }

    private static String getXml(String xml, String path) throws DocumentException {
        Document doc = DocumentHelper.parseText(xml);
        String[] pathA = path.split("\\.");
        Element temp = doc.getRootElement();
        for (int i = 1 ; i <  pathA.length ; i ++) {
            temp = temp.element(pathA[i]);
        }
        return temp.getStringValue();
    }
}
