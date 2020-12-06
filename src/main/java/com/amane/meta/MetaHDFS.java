package com.amane.meta;

import com.amane.adapter.HDFSAdapter;
import com.amane.consts.ConstValue;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public final class MetaHDFS {

    public static void init() {
        BasicConfigurator.configure();
        try {
            String localFilePath = ConstValue.LOCAL_FILE_DIR + "\\" + ConstValue.TEST_FILE_NAME;
            String HDFSFilePath = "/" + ConstValue.HDFS_TEST_DIR + "/" + ConstValue.TEST_FILE_NAME;
            HDFSAdapter hdfsAdapter = new HDFSAdapter();
            hdfsAdapter.mkdir(ConstValue.HDFS_TEST_DIR);
            hdfsAdapter.writeFromLocalFile(localFilePath, HDFSFilePath);
            hdfsAdapter.closeHDFS();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}