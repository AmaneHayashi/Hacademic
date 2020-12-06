package com.amane.demo;

import com.alibaba.fastjson.JSON;
import com.amane.adapter.HDFSAdapter;
import com.amane.bean.database.Paper;
import com.amane.consts.ConstValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class AppendDemo {

    private static String s = "{\"authors\":\"Egon Börger\"," +
            "\"docType\":\"Journal\"," +
            "\"doi\":\"10.1007/BF02011872\"," +
            "\"keywords\":\"Discrete mathematics,Combinatorics,Entscheidungsproblem,Mathematics\"," +
            "\"nCitation\":0," +
            "\"pid\":\"99956490\"," +
            "\"publisher\":\"Springer Science and Business Media LLC\"," +
            "\"title\":\"Bemerkung zu Gurevich's Arbeit über das Entscheidungsproblem für Standardklassen\"," +
            "\"year\":1978}";

    public static void main(String[] args) {
        BasicConfigurator.configure();
        HDFSAdapter hdfsApater = new HDFSAdapter();
        String HDFSFilePath = "/" + ConstValue.HDFS_TEST_DIR + "/" + ConstValue.TEST_FILE_NAME;
        Paper paper = JSON.parseObject(s).toJavaObject(Paper.class);
        String s = '\n' + JSON.toJSONString(paper);
        try {
            hdfsApater.append(Bytes.toBytes(s), HDFSFilePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
