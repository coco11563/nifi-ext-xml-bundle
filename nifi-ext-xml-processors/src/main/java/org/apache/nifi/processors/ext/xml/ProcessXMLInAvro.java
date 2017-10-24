package org.apache.nifi.processors.ext.xml;

import net.sf.saxon.tree.tiny.TinyNodeImpl;
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
import org.apache.nifi.annotation.behavior.DynamicProperty;
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
import org.dom4j.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.nifi.processors.ext.xml.SeparateAvroInXML.CheckUnicodeString;
import static org.apache.nifi.processors.ext.xml.SeparateAvroInXML.UTF8_BOM;

// {[id + basic xml + extend xml + type](每个attr一个ff) -> [id + basic field + extend field(option in dynamic field)]}
@Tags({"Avro","XML","process","Sha0w"})
@CapabilityDescription("通过解析输入的Avro文件中的XML字段内" +
        "（xml field）的指定节点（dynamic field），生成新的Avro文件 更新后版本")
@DynamicProperty(name = "A FlowFile attribute(if <Destination> is set to 'flowfile-attribute'", value = "An XPath expression",
        description = "If <Destination>='flowfile-attribute' "
                + "then the FlowFile attribute is set to the result of " +
                "the XPath Expression.  If <Destination>='flowfile-content' " +
                "then the FlowFile content is set to the result of the XPath Expression.")
public class ProcessXMLInAvro extends AbstractProcessor {
    Logger logger = LoggerFactory.getLogger(ProcessXMLInAvro.class);
    public final static Relationship REL_SUCCESS = new Relationship.Builder()
            .name("sucess")
            .build();

    public final static Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .autoTerminateDefault(true)
            .build();


    public final static PropertyDescriptor NEED_COMPILE_XML_FIELD = new PropertyDescriptor.Builder()
            .name("extend xml field name in avro")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("申明avro中需要解析的EXTEND XML字段名称")
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
        lpd.add(NEED_COMPILE_XML_FIELD);
        propertyDescriptors = Collections.unmodifiableList(lpd);
        Set<Relationship> rs = new HashSet<>();
        rs.add(REL_FAILURE);
        rs.add(REL_SUCCESS);
        relationships = Collections.unmodifiableSet(rs);
    }
    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName).expressionLanguageSupported(false)
                .addValidator(StandardValidators.NON_BLANK_VALIDATOR).required(false).dynamic(true).build();
    }
    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final Map<String, String> dynamicFieldExpressionMap = new HashMap<>();
        final String extendXmlField = context.getProperty(NEED_COMPILE_XML_FIELD).getValue();
        //get dynamic field(only with has extend field option)
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (entry.getKey().isDynamic()) { //get dynamic properties
                dynamicFieldExpressionMap.put(entry.getKey().getName() ,entry.getValue());
            }
        }

        FlowFile flowFile = session.get();
        String type = flowFile.getAttribute("type");
        //{"id","basic xml","extend xml","type"}
        session.read(flowFile, in -> {
            final DataFileStream<GenericRecord> reader = new DataFileStream<>(in, new GenericDatumReader<GenericRecord>());
            GenericRecord currRecord;
            Schema schema = reader.getSchema();
            List<Schema.Field> fieldList = schema.getFields();
            if (schema.getField(extendXmlField) == null) {
                throw new AvroRuntimeException("Not a record: "+this);
            }
            //genericRecord -> map
            final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
            final DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);

            Set<Map<String, String>> keyValue = new HashSet<>(); //存储所有键值对
            Map<String, String> basic;//存储单个键值对
            Set<String> fieldSet = new HashSet<>();//存储所有字段
            fieldSet.addAll(dynamicFieldExpressionMap.keySet()); //将动态属性内的值加入字段集中
            while (reader.hasNext()) {
                currRecord = reader.next();//get single gr
                String extendXml = currRecord.get(extendXmlField).toString();
                basic = new HashMap<>();
                try {
                    basic.putAll(processExtend(extendXml, dynamicFieldExpressionMap));
                } catch (DocumentException e) {
                    e.printStackTrace();
                }
                for (Schema.Field field : fieldList) {
                    if (!Objects.equals(field.name(), extendXmlField) || Objects.equals(field.name(), "type")) {
                        basic.put(field.name(), currRecord.get(field.name()).toString()); //将Avro内其他值加入其中
                    }
                }
                keyValue.add(basic);
            }
            for (Schema.Field field : fieldList) {
                if (!Objects.equals(field.name(), extendXmlField)) {
                    fieldSet.add(field.name());
                }
            }
            Schema newSchema = createSchema(fieldSet, type);

            //passing test
            FlowFile ff = session.create(flowFile);
            final GenericRecord rec = new GenericData.Record(newSchema);
            ff = session.write(ff, out -> {
                final DataFileWriter<GenericRecord> dfw = dataFileWriter.create(newSchema, out);
                for (Map<String,String> m : keyValue) {
                    for (String key : m.keySet()) {
                        logger.error(key + " : " + m.get(key));
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

    public static Map<String, String> processExtend(String xml, Map<String,String> expressionMap) throws DocumentException {
        Document doc;
        try {
            doc = DocumentHelper.parseText(xml);
        } catch (Exception e) {
            String af = CheckUnicodeString(xml);
            doc = DocumentHelper.parseText(af.replaceAll("&#.", " ").replaceAll(UTF8_BOM," "));
        }
        Element rootElem = doc.getRootElement();
        Map<String, String> keyValue = new HashMap<>();
        for (String name : expressionMap.keySet()) {
            String xpath = expressionMap.get(name);
            StringBuilder sb = new StringBuilder();
                if (!xpath.contains("#")) {
                    try {
                        List no = rootElem.selectNodes(xpath);
                        if (no.size() > 1) {
                            for (int i = 0; i < no.size(); i++) {
                                Object o = no.get(i);
                                sb.append(((Node) (o)).getText());
                                if (i < no.size() - 1) {
                                    sb.append("#");
                                }
                            }
                            keyValue.put(name, sb.toString());
                        } else {
                            keyValue.put(name, ((Node) (no.get(0))).getText());
                        }
                    } catch (IndexOutOfBoundsException e) {
                        keyValue.put(name, null);
                    } catch (NullPointerException e) {
                        keyValue.put(name, sb.toString());
                    }
                } else {

                        String[] xpaths = xpath.split("#");
                        for (int i = 0; i < xpaths.length; i++) {
                            try {
                                sb.append(rootElem.selectSingleNode(xpaths[i]).getText());
                            } catch (Exception e) {
                                sb.append("null");
                            }
                            if (i < xpaths.length - 1) {
                                sb.append("#");
                            }
                        }
                        keyValue.put(name, sb.toString());

                }

        }
        return keyValue;
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
