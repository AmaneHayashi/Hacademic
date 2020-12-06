package com.amane.rkmd;

import com.amane.adapter.HDFSAdapter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;


/**
 * 得到用户评分向量
 */
public class Step1 {

    public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException {
        //获得配置信息
        Configuration conf = Recommend.getConf();
        //得到输入输出路径
        Path input = new Path(path.get("Step1Input"));
        Path output = new Path(path.get("Step1Output"));
        //删掉上次输出结果
        HDFSAdapter hdfsAdapter = new HDFSAdapter();
        hdfsAdapter.deleteFile(output.toString());
        //设置作业参数
        Job job = Job.getInstance(conf, "Step1");
        job.setJarByClass(Step1.class);
        job.setMapperClass(Step1_ToItemPreMapper.class);
        job.setCombinerClass(Step1_ToUserVectorReducer.class);
        job.setReducerClass(Step1_ToUserVectorReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        //运行作业
        job.waitForCompletion(true);
    }

    public static class Step1_ToItemPreMapper extends Mapper<Object, Text, Text, Text> {
        private final static Text k = new Text();
        private final static Text v = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = Recommend.DELIMITER.split(value.toString());
            String userID = tokens[0];
            String itemID = tokens[1];
            String pref = tokens[2];
            k.set(userID);
            v.set(itemID + ":" + pref);
            context.write(k, v);
        }
    }

    public static class Step1_ToUserVectorReducer extends Reducer<Text, Text, Text, Text> {
        private final static Text v = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values,
                              Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            StringBuilder sb = new StringBuilder();
            for (Text value : values) {
                sb.append("," + value.toString());
            }
            v.set(sb.toString().replaceFirst(",", ""));
            context.write(key, v);
        }

    }

}
