package com.amane.consts;

import org.apache.hadoop.hbase.TableName;

public interface ConstValue {

    String MASTER_HDFS = "hdfs://192.168.242.198:9000";

    String HDFS_DATA_DIR = "dblp";
    String HDFS_ORG_DATA = "data";
    String HDFS_TEST_DIR = "test";
    String HDFS_RKMD_DIR = "rkmd";
    String HDFS_MRRKMD_DIR = "mrrkmd";
    String HDFS_OUTPUT_DIR = "output";

    String LOCAL_FILE_DIR = "D:\\CodeTemp\\dblp.v12";

    String DATA_FILE_NAME = "data.txt";
    String TEST_FILE_NAME = "test.txt";
    String INDEX_FILE_NAME = "index.txt";
    String RKMD_FILE_NAME = "rkmd.txt";

    String PID_PATTERN = "(?<=\"pid\":\").*?(?=\",)";

    TableName USER_INFO_TABLE = TableName.valueOf("UserInfo");
    TableName USER_BEHAVIOR_TABLE = TableName.valueOf("UserBehavior");
    TableName USER_RECOMMEND_TABLE = TableName.valueOf("UserRecommend");
    TableName PAPER_BACKEND_TABLE = TableName.valueOf("PaperBackend");
    TableName PAPER_INDEX_TABLE = TableName.valueOf("PaperIndex");
    TableName PAPER_RECOMMEND_TABLE = TableName.valueOf("PaperRecommend");

}
