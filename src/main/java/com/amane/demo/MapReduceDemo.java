package com.amane.demo;

/**
 * @author Amane Hayaashi
 * @date 2020/11/28
 * @since 1.0
 */

import com.alibaba.fastjson.JSONObject;
import com.amane.pojo.Paper;
import com.amane.pojo.PaperWritable;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.util.List;

public class MapReduceDemo {

    private static final String MASTER_HDFS = "hdfs://192.168.242.198:9000";

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        BasicConfigurator.configure();
        Path input = new Path(MASTER_HDFS + "/data");
        Path output = new Path(MASTER_HDFS + "/output2");
        Job job = Job.getInstance(conf, "paperTest");
        job.setJarByClass(MapReduceDemo.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(PaperReducer.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(PaperWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class TokenizerMapper extends Mapper<LongWritable, Text, LongWritable, PaperWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String str = value.toString().trim();
            if ("[".equals(str) || "]".equals(str)) {
                return;
            } else {
                if (str.startsWith(",")) {
                    str = str.substring(1);
                }
            }
            Paper paper = JSONObject.parseObject(str).toJavaObject(Paper.class);
            PaperWritable paperWritable = new PaperWritable(paper);
            context.write(new LongWritable(paper.getId()), paperWritable);
        }
    }

    public static class PaperReducer extends Reducer<LongWritable, PaperWritable, LongWritable, Text> {
        public void reduce(LongWritable key, Iterable<PaperWritable> values, Context context) throws IOException, InterruptedException {
            List<PaperWritable> paperWritables = Lists.newArrayList(values);
            for (PaperWritable paperWritable : paperWritables) {
                context.write(key, new Text(paperWritable.getDoi()));
            }
        }
    }
}

