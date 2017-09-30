import org.apache.avro.Schema;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.junit.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.apache.nifi.processors.ext.xml.ProcessProductXML.createSchema;
import static org.apache.nifi.processors.ext.xml.ProcessProductXML.processExtend;
import static org.apache.nifi.processors.ext.xml.ProcessProductXML.processXML;

public class AvroTest {
    private final static String xml = "<product>\n" +
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

    @Test
    public void TestFieldBuilder() throws DocumentException {
        Document doc = DocumentHelper.parseText(xml);
        Element rootElt = doc.getRootElement();
        Set<String> set = new HashSet<>();
        Map<String, String> basic = processXML(rootElt.element("pub_basic"),set);
//        for (String s : basic.keySet()) {
//            System.out.println(s + " : " + basic.get(s));
//        }
        Map<String, String> extend = processXML(processExtend(rootElt,"pub_extend","pub_type_id", "5"), set);
        basic.putAll(extend);
        Set<String> newTestSet = new HashSet<>();
        newTestSet.add("test1");
        newTestSet.add("nonInclude");
        Schema newSchema = createSchema(newTestSet, "123");
        newSchema.getFields().forEach(i -> { //remove the xml field, add the sub title
            if (!Objects.equals(i.name(), "test1")) {
                set.add(i.name());
            }
        });
        set.forEach(System.out::println);

    }
}
