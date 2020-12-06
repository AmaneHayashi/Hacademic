package com.amane.meta;

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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MetaMRIndex {

    private static final Pattern pattern, pattern2;
    private static BufferedWriter out;

    static {
        try {
            String destPath = "D:\\CodeTemp\\dblp.v12\\index.txt";
            out = new BufferedWriter(new FileWriter(destPath));
        } catch (IOException e) {
            e.printStackTrace();
        }
        pattern = Pattern.compile("(?<=\"title\":\").*?(?=\",)");
        pattern2 = Pattern.compile("(?<=\"pid\":\").*?(?=\",)");
    }

    private static void initHDFS() throws Exception {
        Configuration conf = new Configuration();
        BasicConfigurator.configure();
        Path input = new Path(ConstValue.MASTER_HDFS + "/" + ConstValue.HDFS_DATA_DIR);
        Path output = new Path(ConstValue.MASTER_HDFS + "/" + ConstValue.HDFS_OUTPUT_DIR);
        Job job = Job.getInstance(conf, "HDFS2HBase");
        job.setJarByClass(MetaMRPaper.class);
        job.setMapperClass(MRIMapper.class);
        job.setReducerClass(MRIReducer.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        boolean result = job.waitForCompletion(true);
        out.close();
        System.exit(result ? 0 : 1);
    }

    public static class MRIMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

        Text text = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String str = value.toString();
            Matcher matcher = pattern.matcher(str);
            Matcher matcher1 = pattern2.matcher(str);
            if (matcher.find() && matcher1.find()) {
                text.set(matcher1.group(0) + "|" + matcher.group(0));
            }
            context.write(key, text);
        }
    }

    public static class MRIReducer extends Reducer<LongWritable, Text, NullWritable, NullWritable> {
        public void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text text : values) {
                out.write(text.toString());
                out.newLine();
                context.write(NullWritable.get(), NullWritable.get());
            }
        }
    }
}
