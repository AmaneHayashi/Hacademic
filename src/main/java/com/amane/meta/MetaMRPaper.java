package com.amane.meta;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.amane.bean.database.Paper;
import com.amane.bean.database.PaperJson;
import com.amane.consts.ConstValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class MetaMRPaper {

    private static BufferedWriter out;

    static {
        try {
            String destPath = "D:\\CodeTemp\\dblp.v12\\data.txt";
            out = new BufferedWriter(new FileWriter(destPath));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void initHDFS() throws Exception {
        Configuration conf = new Configuration();
        BasicConfigurator.configure();
        Path input = new Path(ConstValue.MASTER_HDFS + "/" + ConstValue.HDFS_ORG_DATA);
        Path output = new Path(ConstValue.MASTER_HDFS + "/" + ConstValue.HDFS_OUTPUT_DIR);
        Job job = Job.getInstance(conf, "HDFS2HBase");
        job.setJarByClass(MetaMRPaper.class);
        job.setMapperClass(MRIMapper.class);
        job.setReducerClass(MRIReducer.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Paper.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        boolean result = job.waitForCompletion(true);
        out.close();
        System.exit(result ? 0 : 1);
    }

    public static class MRIMapper extends Mapper<LongWritable, Text, LongWritable, Paper> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String str = value.toString().trim();
            if ("[".equals(str) || "]".equals(str)) {
                return;
            } else {
                if (str.startsWith(",")) {
                    str = str.substring(1);
                }
            }
            PaperJson paperJson = JSON.toJavaObject(JSONObject.parseObject(str), PaperJson.class);
            Paper paper = new Paper(paperJson);
            context.write(key, paper);
        }
    }

    public static class MRIReducer extends Reducer<LongWritable, Paper, NullWritable, NullWritable> {
        public void reduce(LongWritable key, Iterable<Paper> values, Context context)
                throws IOException, InterruptedException {
            for (Paper paper : values) {
                out.write(JSON.toJSONString(paper));
                out.newLine();
                context.write(NullWritable.get(), NullWritable.get());
            }
        }
    }
}
