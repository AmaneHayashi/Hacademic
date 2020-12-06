package com.amane.rkmd;

import com.amane.consts.ConstValue;
import org.apache.hadoop.conf.Configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * 基于物品的协同推荐系统
 */
public class Recommend {
    //MapReduce分割符为\t和,
    public static final Pattern DELIMITER = Pattern.compile("[\t,]");
    private static final Configuration conf = new Configuration();
    private static Long UTimestamp = 0L;

    public static boolean isNewest(Long timestamp) {
        return UTimestamp != 0L && timestamp.equals(UTimestamp);
    }

    public static void updateTimestamp(Long timestamp) {
        UTimestamp = timestamp;
    }

    /**
     * @throws Exception
     */
    public static Map<String, String> execute() throws Exception {
        String HDFSRkmdPath = ConstValue.MASTER_HDFS + "/" + ConstValue.HDFS_RKMD_DIR;
        String HDFSMRRkmdPath = ConstValue.MASTER_HDFS + "/" + ConstValue.HDFS_MRRKMD_DIR;
        Map<String, String> pathMap = new HashMap<String, String>();
        //本地数据路径
        // pathMap.put("data", "/home/youxingzhi/workspace/Lesson5/small2.csv");
        //步骤1的输入输出路径
        pathMap.put("Step1Input", HDFSRkmdPath);
        pathMap.put("Step1Output", HDFSMRRkmdPath + "/step1");
        //步骤2的输入输出路径
        pathMap.put("Step2Input", pathMap.get("Step1Output"));
        pathMap.put("Step2Output", HDFSMRRkmdPath + "/step2");
        //步骤3_1的输入输出路径
        pathMap.put("Step3Input1", pathMap.get("Step1Output"));
        pathMap.put("Step3Output1", HDFSMRRkmdPath + "/step3_1");
        //步骤3_2的输入输出路径
        pathMap.put("Step3Input2", pathMap.get("Step2Output"));
        pathMap.put("Step3Output2", HDFSMRRkmdPath + "/step3_2");
        //步骤4的输入输出路径
        pathMap.put("Step4_1Input1", pathMap.get("Step3Output1"));
        pathMap.put("Step4_1Input2", pathMap.get("Step3Output2"));
        pathMap.put("Step4_1Output", HDFSMRRkmdPath + "/step4_1");
        pathMap.put("Step4_2Input", pathMap.get("Step4_1Output"));
        pathMap.put("Step4_2Output", HDFSMRRkmdPath + "/step4_2");
        //步骤5的输入输出路径
        pathMap.put("Step5Input1", pathMap.get("Step4_2Output"));
        pathMap.put("Step5Input2", HDFSRkmdPath);
        pathMap.put("Step5Output", HDFSMRRkmdPath + "/step5");

        Step1.run(pathMap);
        Step2.run(pathMap);
        Step3.run1(pathMap);
        Step3.run2(pathMap);
        Step4_Update.run(pathMap);
        Step4_Update2.run(pathMap);
        return Step5.run(pathMap);
    }

    public static Configuration getConf() {
        return conf;
    }
}
