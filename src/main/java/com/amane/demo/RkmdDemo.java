package com.amane.demo;

import com.amane.adapter.HDFSAdapter;
import com.amane.adapter.RkmdAdapter;
import com.amane.consts.ConstValue;
import com.amane.tools.RkmdTools;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class RkmdDemo {

    private static final String HDFSRkmdPath = ConstValue.MASTER_HDFS + "/" + ConstValue.HDFS_RKMD_DIR;
    private static final String localRkmdPath = ConstValue.LOCAL_FILE_DIR + "\\" + ConstValue.RKMD_FILE_NAME;
    private static final String indexPath = ConstValue.LOCAL_FILE_DIR + "\\" + ConstValue.INDEX_FILE_NAME;

    private static final String[] userArr
            = new String[]{"user1", "user2", "user3", "user4", "user5", "user6", "user7", "user8", "user9", "user10"};

    public static void main(String[] args) throws Exception {
        //BasicConfigurator.configure();
        Map<String, Integer> map = new HashMap<>();
        genTestFile();
        HDFSAdapter hdfsAdapter = new HDFSAdapter();
        hdfsAdapter.deleteFile(HDFSRkmdPath + "/" + ConstValue.RKMD_FILE_NAME);
        hdfsAdapter.writeFromLocalFile(localRkmdPath, HDFSRkmdPath);
        hdfsAdapter.closeHDFS();
        System.out.println(Calendar.getInstance().getTime());
        Map<String, String> URkmdMap = RkmdAdapter.executeURkmd();
        for (Map.Entry<String, String> entry : URkmdMap.entrySet()) {
            System.out.println(entry.toString());
        }
        System.out.println(Calendar.getInstance().getTime());
    }

    public static void genTestFile() throws IOException {
        int feed = 20;
        List<String> pidList = new ArrayList<>();
        LineIterator lineIterator = FileUtils.lineIterator(new File(indexPath), "utf-8");
        BufferedWriter out = new BufferedWriter(new FileWriter(localRkmdPath));
        while (lineIterator.hasNext()) {
            double d = Math.random();
            String result = lineIterator.next();
            if (d < 0.33) {
                String pid = result.substring(0, result.indexOf("|"));
                pidList.add(pid);
            }
            if (pidList.size() >= 50) {
                break;
            }
        }
        for (String u : userArr) {
            for (int i = 0; i < feed; i++) {
                String pid = pidList.get((int) (Math.random() * 50));
                double star = RkmdTools.getRandomStar();
                String s = String.format("%s,%s,%s", u, pid, star);
                out.write(s);
                out.newLine();
            }
        }
        out.close();
    }
}
