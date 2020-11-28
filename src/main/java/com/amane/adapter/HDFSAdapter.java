package com.amane.adapter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * @author Amane Hayaashi
 * @date 2020/11/28
 * @since 1.0
 */

public class HDFSAdapter {

    private FileSystem HDFS;

    public HDFSAdapter(Configuration conf) throws IOException {
        this.HDFS = FileSystem.get(conf);
    }

    public void mkdir(String HDFSDir) throws IOException {
        if (!HDFS.exists(new Path("/" + HDFSDir))) {
            HDFS.mkdirs(new Path("/" + HDFSDir));
        }
    }

    public void writeFromLocalFile(String localFilePath, String HDFSFilePath) throws IOException {
        Path src = new Path(localFilePath);
        Path dest = new Path(HDFSFilePath);
        HDFS.copyFromLocalFile(src, dest);
    }

    public void close() throws IOException {
        HDFS.close();
    }

    public void writeTest(String HDFSDir) {
        String fileName = "/" + HDFSDir + "/" +
                (new SimpleDateFormat("yyyy-MM-dd").format(Calendar.getInstance().getTime())) + ".txt";
        String fileContent = System.currentTimeMillis() + "\n";
        Path dst = new Path(fileName);
        try {
            if (!HDFS.exists(dst)) {
                FSDataOutputStream output = HDFS.create(dst);
                output.close();
            }
            InputStream in = new ByteArrayInputStream(fileContent.getBytes(StandardCharsets.UTF_8));
            OutputStream out = HDFS.append(dst);
            IOUtils.copyBytes(in, out, 4096, true);
        } catch (Exception e) {
            e.printStackTrace();
            try {
                HDFS.close();
            } catch (IOException f) {
                f.printStackTrace();
            }
        }
    }
}
