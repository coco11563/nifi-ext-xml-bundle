package org.apache.nifi.processors.ext.xml.Bean;

import org.dom4j.Document;
import org.dom4j.Element;

/**
 * simple way to store xml path information
 */
public class XMLExpression {
    private String[] path;
    private XMLExpression(String[] path) {
        setPath(path);
    }
    public static XMLExpression parse(String s) throws Exception {
        if (s == null) {
            throw new Exception();
        }
        String[] re = s.split("\\.");
        if (re.length < 1) throw new Exception();
        return new XMLExpression(re);
    }

    public String[] getPath() {
        return path;
    }

    private void setPath(String[] path) {
        this.path = path;
    }

}
