package com.amane.adapter;

import com.alibaba.fastjson.JSONObject;
import com.amane.consts.ConstValue;
import com.amane.tools.BeanTools;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class MRAdapter {

    private static List<Long> targetList;
    private static List<String> resultList;
    private static boolean keepFlag = true;

    public static <T> List<T> findBy(List<Long> pidList, Class<T> clazz) throws Exception {
        targetList = pidList;
        Collections.sort(targetList);
        resultList = new ArrayList<>();
        MRFind();
        return resultList.stream()
                .map(s -> JSONObject.toJavaObject(JSONObject.parseObject(s), clazz))
                .collect(Collectors.toList());
    }

    private static void MRFind() throws Exception {
        Configuration conf = new Configuration();
        Path input = new Path(ConstValue.MASTER_HDFS + "/" + ConstValue.HDFS_DATA_DIR);
        Path output = new Path(ConstValue.MASTER_HDFS + "/" + ConstValue.HDFS_OUTPUT_DIR);
        Job job = Job.getInstance(conf, "HDFS2HBase");
        job.setJarByClass(MRAdapter.class);
        job.setMapperClass(MRFindMapper.class);
        job.setNumReduceTasks(0);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        // 当拿到全部结果时直接杀死Job
        new Thread(() -> {
            while (keepFlag) {
                try {
                    Thread.sleep(500);
                    if (!keepFlag) {
                        job.killJob();
                    }
                } catch (IOException | InterruptedException ignored) {
                }
            }
        }).start();
        job.waitForCompletion(true);
    }

    private static class MRFindMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String s = value.toString();
            if (BeanTools.anyContains(s, targetList)) {
                resultList.add(s);
            }
            if (targetList.isEmpty()) {
                keepFlag = false;
                return;
            }
            context.write(NullWritable.get(), NullWritable.get());
        }
    }
}
