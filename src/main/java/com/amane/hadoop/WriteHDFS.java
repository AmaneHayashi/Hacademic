package com.amane.hadoop;

import com.amane.adapter.HDFSAdapter;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;


/**
 * @author Amane Hayaashi
 * @date 2020/11/28
 * @since 1.0
 */

public final class WriteHDFS {

    private static final String MASTER_HDFS = "hdfs://192.168.242.198:9000";
    private static final String HDFS_DIR = "data";
    private static final String LOCAL_FILE_DIR = "D:\\CodeTemp\\dblp.v12";
    private static final String LOCAL_FILE_NAME = "dblp.v12.json";

    public static void main(String[] args) {
        BasicConfigurator.configure();
        Configuration HDFSConf = new Configuration();
        try {
            HDFSConf.set("fs.defaultFS", MASTER_HDFS);
            HDFSConf.set("dfs.support.append", "true");//HDFS文件中追加内容
            HDFSConf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
            HDFSConf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            String localFilePath = LOCAL_FILE_DIR + "\\" + LOCAL_FILE_NAME;
            String HDFSFilePath = "/" + HDFS_DIR + "/" + LOCAL_FILE_NAME;
            HDFSAdapter hdfsAdapter = new HDFSAdapter(HDFSConf);
            hdfsAdapter.mkdir(HDFS_DIR);
            hdfsAdapter.writeFromLocalFile(localFilePath, HDFSFilePath);
            hdfsAdapter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}