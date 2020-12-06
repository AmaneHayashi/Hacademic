package com.amane.rkmd;

import com.amane.adapter.HDFSAdapter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * 对结果进行过滤和排序
 * 1过滤掉用户已经打过分的
 * 2按推荐权重倒序排列
 */
public class Step5 {

    private static final int RKMD_LENGTH = 10;

    private static final Map<String, String> result = new HashMap<>();
    private static final Map<String, Integer> limit = new HashMap<>();

    public static Map<String, String> run(Map<String, String> path)
            throws IOException, InterruptedException, ClassNotFoundException {
        // 获得配置信息
        Configuration conf = Recommend.getConf();
        // 得到输入输出路径
        Path input1 = new Path(path.get("Step5Input1"));
        Path input2 = new Path(path.get("Step5Input2"));
        Path output = new Path(path.get("Step5Output"));
        // 删除上一次的输出
        HDFSAdapter hdfsAdapter = new HDFSAdapter();
        hdfsAdapter.deleteFile(output.toString());
        // 设置作业参数
        Job job = Job.getInstance(conf);
        job.setJarByClass(Step5.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Step5_FilterSortMapper.class);
        job.setReducerClass(Step5_FilterSortReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, input1, input2);
        FileOutputFormat.setOutputPath(job, output);

        job.waitForCompletion(true);
        // 设置输出
        return result;
    }

    public static class Step5_FilterSortMapper extends Mapper<LongWritable, Text, Text, Text> {
        private String flag;// 判断输入的文件
        private Text k;
        private Text v;

        @Override
        protected void setup(
                Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();
            flag = split.getPath().getParent().getName();// 判断读的数据集
        }

        @Override
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {

            if (flag.equals("step4_2")) {//values like 1	101,44.0
                String[] tokens = Recommend.DELIMITER.split(values.toString());
                k = new Text(tokens[0]);
                v = new Text("W:" + tokens[1] + "," + tokens[2]);//为推荐权重
            } else {
                String[] tokens = Recommend.DELIMITER.split(values.toString());
                k = new Text(tokens[0]);
                v = new Text("S:" + tokens[1] + "," + tokens[2]);//为分数
            }
            context.write(k, v);
        }
    }

    public static class Step5_FilterSortReducer extends Reducer<Text, Text, Text, Text> {
        private Text k;
        private Text v;

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashMap<String, String> wMap = new HashMap<String, String>();
            HashMap<String, String> sMap = new HashMap<String, String>();

            for (Text line : values) {
                String[] tokens = Recommend.DELIMITER.split(line.toString());
                String flag = tokens[0].split(":")[0];
                String itemID = tokens[0].split(":")[1];
                if (flag.equals("W")) {
                    wMap.put(itemID, tokens[1]);
                } else {
                    sMap.put(itemID, tokens[1]);
                }
            }
            //过滤
            HashMap<String, Float> filterMap = new HashMap<String, Float>();
            for (String k : wMap.keySet()) {
                if (!sMap.containsKey(k))
                    filterMap.put(k, Float.valueOf(wMap.get(k)));
            }
            //排序
            List<Entry<String, Float>> list = new LinkedList<Entry<String, Float>>();
            list = SortHashMap.sortHashMap(filterMap);
            for (Entry<String, Float> l : list) {
                k = key;
                v = new Text(l.getKey().toString() + "," + l.getValue().toString());
                context.write(k, v);
                // 输出参数
                String ks = k.toString();
                String vs = l.getKey();
                if (result.containsKey(ks)) {
                    int length = limit.get(ks);
                    if (length < RKMD_LENGTH) {
                        String val = result.get(ks);
                        String vk = val.concat(",").concat(vs);
                        limit.put(ks, ++length);
                        result.put(ks, vk);
                    }
                } else {
                    limit.put(ks, 1);
                    result.put(ks, vs);
                }
            }
        }
    }
}
