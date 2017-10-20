package xml;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processors.ext.xml.ProcessXMLInAvro;
import org.apache.nifi.processors.ext.xml.SeparateAvroByXML;
import org.apache.nifi.processors.ext.xml.SeparateAvroInXML;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class XMLProcessorTest {


    private GenericRecord gr;

    final private Set<GenericRecord> setGr = new HashSet<>();;

    final private InputStream flowFile = null;

    final private TestRunner testRunnerSPX = TestRunners.newTestRunner(new SeparateAvroInXML());

    final private TestRunner testRunnerPPX = TestRunners.newTestRunner(new ProcessXMLInAvro());

    final private MockFlowFile mff = null;

    public final GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(
            grSchema);

    public final DataFileWriter <GenericRecord> writer = new DataFileWriter<>(
            new GenericDatumWriter<>());

    private byte[] buffer ;
    private DataFileReader dr;
    @Before
    public void init() throws IOException {
        dr = new DataFileReader<GenericData>(new File("C:\\Users\\sha0w\\IdeaProjects\\nifi-ext-xml-bundle\\nifi-ext-xml-processors\\src\\main\\resources\\2269418313656416.avro"), new GenericDatumReader<>());
        //done gr init
        File f = new File(
                "C:\\Users\\sha0w\\IdeaProjects\\" +
                        "nifi-ext-xml-bundle\\nifi-ext-xml-processors\\src\\main\\resources" +
                        "\\2269418313656416.avro");
        FileInputStream fis = new FileInputStream(f);
        BufferedInputStream bfs = new BufferedInputStream(fis);

        DataFileStream<GenericRecord> dfs = new DataFileStream<GenericRecord>(bfs , new GenericDatumReader<GenericRecord>());
        System.out.println(dfs.getSchema().getFields().size());
    }
    @Test
    public void testRunnerSPX() throws IOException {
//        DatumReader<GenericRecord> reader=new GenericDatumReader<GenericRecord>(grSchema);
//        Decoder decoder= DecoderFactory.get().binaryDecoder(buffer,null);
//        GenericRecord result=reader.read(null,decoder);
//        System.out.println(result.getSchema());
        testRunnerSPX.setProperty(SeparateAvroByXML.XML_DECODE_FIELD,"product_xml");
        testRunnerSPX.setProperty(SeparateAvroByXML.XML_TYPE_FIELD,"/product/pub_basic/pub_type_id");
//        testRunnerSPX.setProperty(SeparateAvroByXML.XML_COMMON_FIELD, "/product/pub_basic");
//        testRunnerSPX.setProperty(SeparateAvroByXML.XML_UNIQUE_FIELD, "/product/pub_extend");
//        testRunnerSPX.setProperty(SeparateAvroByXML.XML_TYPE_FIELD_NAME, "pub_type_id");
//        File f = new File(
//                "C:\\Users\\sha0w\\IdeaProjects\\" +
//                        "nifi-ext-xml-bundle\\nifi-ext-xml-processors\\src\\main\\resources" +
//                        "\\2269418313656416.avro");
        List<File> fileList = new ArrayList<>();

//        fileList.add(new File("C:\\Users\\sha0w\\IdeaProjects\\nifi-ext-xml-bundle\\nifi-ext-xml-processors\\src\\main\\resources\\2254594220071158.avro"));
//        fileList.add(new File("C:\\Users\\sha0w\\IdeaProjects\\nifi-ext-xml-bundle\\nifi-ext-xml-processors\\src\\main\\resources\\2254594220071158 (1).avro"));
//        fileList.add(new File("C:\\Users\\sha0w\\IdeaProjects\\nifi-ext-xml-bundle\\nifi-ext-xml-processors\\src\\main\\resources\\2254594220071158 (2).avro"));
//        fileList.add(new File("C:\\Users\\sha0w\\IdeaProjects\\nifi-ext-xml-bundle\\nifi-ext-xml-processors\\src\\main\\resources\\2254594220071158 (3).avro"));
//        fileList.add(new File("C:\\Users\\sha0w\\IdeaProjects\\nifi-ext-xml-bundle\\nifi-ext-xml-processors\\src\\main\\resources\\2254594220071158 (4).avro"));
//        fileList.add(new File("C:\\Users\\sha0w\\IdeaProjects\\nifi-ext-xml-bundle\\nifi-ext-xml-processors\\src\\main\\resources\\2254594220071158 (5).avro"));
        fileList.add(new File("C:\\Users\\sha0w\\IdeaProjects\\nifi-ext-xml-bundle\\nifi-ext-xml-processors\\src\\main\\resources\\2254594220071158 (6).avro"));

//        FileInputStream fis = new FileInputStream(f);
//        BufferedInputStream bfs = new BufferedInputStream(fis);

//        testRunnerSPX.enqueue(bfs);
        for (File fi : fileList) {
            FileInputStream fisL = new FileInputStream(fi);
            BufferedInputStream bfsL = new BufferedInputStream(fisL);
            testRunnerSPX.enqueue(bfsL);
        }
        testRunnerSPX.run();


        testRunnerPPX.setProperty(ProcessXMLInAvro.NEED_COMPILE_XML_FIELD, "need_d");


        List<MockFlowFile> mff = testRunnerSPX.getFlowFilesForRelationship(SeparateAvroByXML.REL_SUCCESS);
        for (FlowFile ff : mff) {
            System.out.println(ff.toString());
        }
    }

    private final static String xml_5 = "<product>\n" +
            "    <pub_basic>\n" +
            "        <pub_id>1000013512222</pub_id>\n" +
            "        <pub_type_id>5</pub_type_id>\n" +
            "        <zh_pub_type_name>专利</zh_pub_type_name>\n" +
            "        <en_pub_type_name>Patent</en_pub_type_name>\n" +
            "        <zh_title>一种基于哈希双向认证的无线传感网络定位安全方法</zh_title>\n" +
            "        <en_title></en_title>\n" +
            "        <zh_source>2015/4/8, 江苏, ( 0 中华人民共和国国家知识产权局, CN201410782399.3.</zh_source>\n" +
            "        <en_source>8/4/2015, 江苏, ( 0 中华人民共和国国家知识产权局, CN201410782399.3.</en_source>\n" +
            "        <authors_name/>\n" +
            "        <publish_year>2014</publish_year>\n" +
            "        <publish_month>12</publish_month>\n" +
            "        <publish_day>16</publish_day>\n" +
            "        <create_date>2015/11/23 09:52:17</create_date>\n" +
            "        <has_full_text>0</has_full_text>\n" +
            "        <full_text_img_url/>\n" +
            "        <list_ei_source>0</list_ei_source>\n" +
            "        <list_sci_source>0</list_sci_source>\n" +
            "        <list_ssci_source>0</list_ssci_source>\n" +
            "        <list_istp_source>0</list_istp_source>\n" +
            "        <list_ei>0</list_ei>\n" +
            "        <list_sci>0</list_sci>\n" +
            "        <list_ssci>0</list_ssci>\n" +
            "        <list_istp>0</list_istp>\n" +
            "        <owner>0</owner>\n" +
            "        <authenticated>1</authenticated>\n" +
            "        <cited_times/>\n" +
            "        <pub_detail_param>ZeNOV1oeLN81eODOD7Ss9Q%3D%3D</pub_detail_param>\n" +
            "        <full_link/>\n" +
            "        <product_mark/>\n" +
            "        <authors/>\n" +
            "        <public_date>2014-12-16</public_date>\n" +
            "    </pub_basic>\n" +
            "    <pub_extend pub_type_id=\"1\">\n" +
            "        <award_type_name/>\n" +
            "        <award_grade_name/>\n" +
            "        <prize_org/>\n" +
            "    </pub_extend>\n" +
            "    <pub_extend pub_type_id=\"2\">\n" +
            "        <language/>\n" +
            "        <publication_status/>\n" +
            "        <country_name>江苏</country_name>\n" +
            "        <city>210023 江苏省南京市亚东新城区文苑路9号</city>\n" +
            "        <pub_house/>\n" +
            "        <t_word/>\n" +
            "        <isbn/>\n" +
            "    </pub_extend>\n" +
            "    <pub_extend pub_type_id=\"3\">\n" +
            "        <conf_name/>\n" +
            "        <conf_type/>\n" +
            "        <doi/>\n" +
            "        <conf_org/>\n" +
            "        <conf_start_year/>\n" +
            "        <conf_start_month/>\n" +
            "        <conf_start_day/>\n" +
            "        <conf_end_year/>\n" +
            "        <conf_end_month/>\n" +
            "        <conf_end_day/>\n" +
            "        <begin_num/>\n" +
            "        <end_num/>\n" +
            "        <paper_type/>\n" +
            "        <country_name>江苏</country_name>\n" +
            "        <city>210023 江苏省南京市亚东新城区文苑路9号</city>\n" +
            "        <article_no/>\n" +
            "    </pub_extend>\n" +
            "    <pub_extend pub_type_id=\"4\">\n" +
            "        <impact_factors/>\n" +
            "        <public_status/>\n" +
            "        <doi/>\n" +
            "        <issue_no code=\"01\"/>\n" +
            "        <issue_no code=\"02\"/>\n" +
            "        <include_start/>\n" +
            "        <begin_num/>\n" +
            "        <end_num/>\n" +
            "        <article_no/>\n" +
            "        <journal_name/>\n" +
            "    </pub_extend>\n" +
            "    <pub_extend pub_type_id=\"5\">\n" +
            "        <patent_status/>\n" +
            "        <apply_man>南京邮电大学</apply_man>\n" +
            "        <license_unit>( 0 中华人民共和国国家知识产权局</license_unit>\n" +
            "        <ch_patent_type>发明专利</ch_patent_type>\n" +
            "        <patent_num>CN201410782399.3</patent_num>\n" +
            "        <country_name>江苏</country_name>\n" +
            "        <city>210023 江苏省南京市亚东新城区文苑路9号</city>\n" +
            "        <patent>其他</patent>\n" +
            "        <patent_name>其他国家专利</patent_name>\n" +
            "        <qt_patent_country>江苏：发明专利</qt_patent_country>\n" +
            "    </pub_extend>\n" +
            "    <pub_extend pub_type_id=\"7\">\n" +
            "        <country_name>江苏</country_name>\n" +
            "        <city>210023 江苏省南京市亚东新城区文苑路9号</city>\n" +
            "    </pub_extend>\n" +
            "    <pub_extend pub_type_id=\"10\">\n" +
            "        <book_name/>\n" +
            "        <series_book/>\n" +
            "        <isbn/>\n" +
            "        <editors/>\n" +
            "        <country_name>江苏</country_name>\n" +
            "        <city>210023 江苏省南京市亚东新城区文苑路9号</city>\n" +
            "        <pub_house/>\n" +
            "    </pub_extend>\n" +
            "</product>\n";

    private final static String xml_4 = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<product>\n" +
            "<pub_basic>\n" +
            "<pub_id>1000002720930</pub_id>\n" +
            "<pub_type_id>3</pub_type_id>\n" +
            "<zh_pub_type_name>会议论文</zh_pub_type_name>\n" +
            "<en_pub_type_name>Conference Paper</en_pub_type_name>\n" +
            "<zh_title>Identification and location of the pigment granules in the retinal pigment epithelium cells using fluorescence technology</zh_title>\n" +
            "<en_title>Identification and location of the pigment granules in the retinal pigment epithelium cells using fluorescence technology</en_title>\n" +
            "<zh_source>International Symposium on Biophotonics, Nanophotonics and Metamaterials, Metamaterials 2006, 16/10/2006-18/10/2006, pp 68-71, Hangzhou, China, 2006.</zh_source>\n" +
            "<en_source>International Symposium on Biophotonics, Nanophotonics and Metamaterials, Metamaterials 2006, 16/10/2006-18/10/2006, pp 68-71, Hangzhou, China, 2006.</en_source>\n" +
            "<authors_name>*Xu, Gaixia; Qu, Junle; Sun, Yiwen; Zhao, Lingling; Ding, Zhihua; Niu, Hanben</authors_name>\n" +
            "<publish_year>2006</publish_year>\n" +
            "<publish_month/>\n" +
            "<publish_day/>\n" +
            "<create_date>2012/02/29 13:12:55</create_date>\n" +
            "<has_full_text>0</has_full_text>\n" +
            "<full_text_img_url/>\n" +
            "<list_ei_source>1</list_ei_source>\n" +
            "<list_sci_source>0</list_sci_source>\n" +
            "<list_ssci_source>0</list_ssci_source>\n" +
            "<list_istp_source>0</list_istp_source>\n" +
            "<list_ei>1</list_ei>\n" +
            "<list_sci>0</list_sci>\n" +
            "<list_ssci>0</list_ssci>\n" +
            "<list_istp>0</list_istp>\n" +
            "<owner>0</owner>\n" +
            "<authenticated>1</authenticated>\n" +
            "<cited_times/>\n" +
            "<pub_detail_param>q54wbzr6Wc4UURct2aCHFw%3D%3D</pub_detail_param>\n" +
            "<full_link/>\n" +
            "<product_mark/>\n" +
            "<authors>\n" +
            "<author>\n" +
            "<psn_name>Xu, Gaixia</psn_name>\n" +
            "<org_name/>\n" +
            "<email/>\n" +
            "<is_message>1</is_message>\n" +
            "<first_author>0</first_author>\n" +
            "<is_mine>否</is_mine>\n" +
            "</author>\n" +
            "<author>\n" +
            "<psn_name>Qu, Junle</psn_name>\n" +
            "<org_name/>\n" +
            "<email/>\n" +
            "<is_message>0</is_message>\n" +
            "<first_author>0</first_author>\n" +
            "<is_mine>否</is_mine>\n" +
            "</author>\n" +
            "<author>\n" +
            "<psn_name>Sun, Yiwen</psn_name>\n" +
            "<org_name/>\n" +
            "<email/>\n" +
            "<is_message>0</is_message>\n" +
            "<first_author>0</first_author>\n" +
            "<is_mine>否</is_mine>\n" +
            "</author>\n" +
            "<author>\n" +
            "<psn_name>Zhao, Lingling</psn_name>\n" +
            "<org_name/>\n" +
            "<email/>\n" +
            "<is_message>0</is_message>\n" +
            "<first_author>0</first_author>\n" +
            "<is_mine>否</is_mine>\n" +
            "</author>\n" +
            "<author>\n" +
            "<psn_name>Ding, Zhihua</psn_name>\n" +
            "<org_name/>\n" +
            "<email/>\n" +
            "<is_message>0</is_message>\n" +
            "<first_author>0</first_author>\n" +
            "<is_mine>否</is_mine>\n" +
            "</author>\n" +
            "<author>\n" +
            "<psn_name>Niu, Hanben</psn_name>\n" +
            "<org_name/>\n" +
            "<email/>\n" +
            "<is_message>0</is_message>\n" +
            "<first_author>0</first_author>\n" +
            "<is_mine>否</is_mine>\n" +
            "</author>\n" +
            "</authors>\n" +
            "<public_date>2006-01-01</public_date></pub_basic>\n" +
            "<pub_extend pub_type_id=\"1\">\n" +
            "<award_type_name/>\n" +
            "<award_grade_name/>\n" +
            "<prize_org/>\n" +
            "</pub_extend>\n" +
            "<pub_extend pub_type_id=\"2\">\n" +
            "<language/>\n" +
            "<publication_status/>\n" +
            "<country_name>China</country_name>\n" +
            "<city>Hangzhou, China</city>\n" +
            "<pub_house/>\n" +
            "<t_word/>\n" +
            "<isbn/>\n" +
            "</pub_extend>\n" +
            "<pub_extend pub_type_id=\"3\">\n" +
            "<conf_name>International Symposium on Biophotonics, Nanophotonics and Metamaterials, Metamaterials 2006</conf_name>\n" +
            "<conf_type/>\n" +
            "<doi>10.1109/METAMAT.2006.335000</doi>\n" +
            "<conf_org/>\n" +
            "<conf_start_year>2006</conf_start_year>\n" +
            "<conf_start_month>10</conf_start_month>\n" +
            "<conf_start_day>16</conf_start_day>\n" +
            "<conf_end_year>2006</conf_end_year>\n" +
            "<conf_end_month>10</conf_end_month>\n" +
            "<conf_end_day>18</conf_end_day>\n" +
            "<begin_num>68</begin_num>\n" +
            "<end_num>71</end_num>\n" +
            "<paper_type/>\n" +
            "<country_name>China</country_name>\n" +
            "<city>Hangzhou, China</city>\n" +
            "<article_no/>\n" +
            "<conf_start_date>2006-10-16</conf_start_date><conf_end_date>2006-10-18</conf_end_date><product_mark>0</product_mark><product_mark_name>未标注</product_mark_name></pub_extend>\n" +
            "<pub_extend pub_type_id=\"4\">\n" +
            "<impact_factors/>\n" +
            "<public_status/>\n" +
            "<doi>10.1109/METAMAT.2006.335000</doi>\n" +
            "<issue_no code=\"01\"/>\n" +
            "<issue_no code=\"02\"/>\n" +
            "<include_start/>\n" +
            "<begin_num>68</begin_num>\n" +
            "<end_num>71</end_num>\n" +
            "<article_no/>\n" +
            "<journal_name/>\n" +
            "</pub_extend>\n" +
            "<pub_extend pub_type_id=\"5\">\n" +
            "<patent_status/>\n" +
            "<apply_man/>\n" +
            "<license_unit/>\n" +
            "<ch_patent_type/>\n" +
            "<patent_num/>\n" +
            "<country_name>China</country_name>\n" +
            "<city>Hangzhou, China</city>\n" +
            "</pub_extend>\n" +
            "<pub_extend pub_type_id=\"7\">\n" +
            "<country_name>China</country_name>\n" +
            "<city>Hangzhou, China</city>\n" +
            "</pub_extend>\n" +
            "<pub_extend pub_type_id=\"10\">\n" +
            "<book_name/>\n" +
            "<series_book/>\n" +
            "<isbn/>\n" +
            "<editors/>\n" +
            "<country_name>China</country_name>\n" +
            "<city>Hangzhou, China</city>\n" +
            "<pub_house/>\n" +
            "</pub_extend>\n" +
            "</product>";

    final private static Schema grSchema = Schema.parse("{\n" +
            "   \"type\" : \"record\",\n" +
            "   \"namespace\" : \"Tutorialspoint\",\n" +
            "   \"name\" : \"testName\",\n" +
            "   \"fields\" : [\n" +
            "      { \"name\" : \"testfield1\" , \"type\" : [\"string\",\"null\"] },\n" +
            "      { \"name\" : \"testfield2\" , \"type\" : [\"string\",\"null\"] },\n" +
            "      { \"name\" : \"need_d\" , \"type\" :  [\"string\",\"null\"] }\n" +
            "   ]\n" +
            "}");
}
