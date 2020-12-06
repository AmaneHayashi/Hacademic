package com.amane.adapter;

import com.amane.consts.ConstValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class HDFSAdapter {

    private static final Configuration HDFSConf;

    static {
        HDFSConf = new Configuration();
        HDFSConf.set("fs.defaultFS", ConstValue.MASTER_HDFS);
        HDFSConf.set("dfs.support.append", "true");
        HDFSConf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        HDFSConf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
    }

    private final FileSystem HDFS;

    public HDFSAdapter() {
        try {
            this.HDFS = FileSystem.get(HDFSConf);
        } catch (IOException e) {
            throw new RuntimeException("initialize HDFS failed");
        }
    }

    public boolean exists(String HDFSDir) throws IOException {
        return HDFS.exists(new Path("/" + HDFSDir));
    }

    public void mkdir(String HDFSDir) throws IOException {
        if (!exists(HDFSDir)) {
            HDFS.mkdirs(new Path("/" + HDFSDir));
        }
    }

    public void writeFromLocalFile(String localFilePath, String HDFSFilePath) throws IOException {
        Path src = new Path(localFilePath);
        Path dest = new Path(HDFSFilePath);
        HDFS.copyFromLocalFile(src, dest);
    }

    public void writeToLocalFile(String localFilePath, String HDFSFilePath) throws IOException {
        Path src = new Path(localFilePath);
        Path dest = new Path(HDFSFilePath);
        HDFS.copyToLocalFile(src, dest);
    }

    public void read(String HDFSFilePath) throws IOException {
        FSDataInputStream fsDataInputStream = null;
        Path path = new Path(HDFSFilePath);
        fsDataInputStream = HDFS.open(path);
        IOUtils.copyBytes(fsDataInputStream, System.out, 4096, false);
        IOUtils.closeStream(fsDataInputStream);
    }

    public void rename(String oldPath, String newPath) throws IOException {
        HDFS.rename(new Path(oldPath), new Path(newPath));
    }

    public void append(byte[] bytes, String HDFSFilePath) throws IOException {
        InputStream in = new ByteArrayInputStream(bytes);
        OutputStream out = HDFS.append(new Path(HDFSFilePath));
        IOUtils.copyBytes(in, out, 4096, true);
    }

    public void appendFromLocalFile(String localFilePath, String HDFSFilePath) throws IOException {
        InputStream in = new BufferedInputStream(new FileInputStream(localFilePath));
        OutputStream out = HDFS.append(new Path(HDFSFilePath));
        IOUtils.copyBytes(in, out, 4096, true);
    }

    public void deleteFile(String HDFSFilePath) throws IOException {
        Path path = new Path(HDFSFilePath);
        if (HDFS.exists(path)) {
            HDFS.delete(path, true);
        }
    }

    public void closeHDFS() throws IOException {
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
