package com.amane.demo;

/**
 * @author Amane Hayaashi
 * @date 2020/11/24
 * @since 1.0
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;

/**
 * （1）用户编写的程序分成三个部分：Mapper，Reducer，Driver(提交运行mr程序的客户端)
 * <p>
 * （2）Mapper的输入数据是KV对的形式（KV的类型可自定义）
 * <p>
 * （3）Mapper的输出数据是KV对的形式（KV的类型可自定义）
 * <p>
 * （4）Mapper中的业务逻辑写在map()方法中
 * <p>
 * （5）map()方法（maptask进程）对每一个<K,V>调用一次
 * <p>
 * （6）Reducer的输入数据类型对应Mapper的输出数据类型，也是KV
 * <p>
 * （7）Reducer的业务逻辑写在reduce()方法中
 * <p>
 * （8）Reducetask进程对每一组相同k的<k,v>组调用一次reduce()方法
 * <p>
 * （9）用户自定义的Mapper和Reducer都要继承各自的父类
 * <p>
 * （10）整个程序需要一个Drvier来进行提交，提交的是一个描述了各种必要信息的job对象
 */

public class WordCount {

    private static final String IP = "192.168.242.198";

    /*
     * 为啥要main方法：先启动自己，然后把整个程序提交到集群
     * 相当于yarn集群的客户端，yarn去分配运算资源，然后才能启动
     * 以为是yarn的客户端，所以要再次封装mr程序的相关运行参数，指定jar包
     * 最后提交给yarn
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        /*conf.set("mapreduce.framework.name", "yarn");
		conf.set("yarn.resourcemanager.hostname","hadoop01");*/

        BasicConfigurator.configure();

        Path input = new Path(URI.create(String.format("hdfs://%s:9000/input", IP)));
        Path output = new Path(URI.create(String.format("hdfs://%s:9000/output2", IP)));
        //给一些默认的参数
        Job job = Job.getInstance(conf, "word count");
        //指定本程序的jar包所在的本地路径  把jar包提交到yarn
        job.setJarByClass(WordCount.class);
        /*
         * 告诉框架调用哪个类
         * 指定本业务job要是用的mapper/Reducer业务类
         */
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        //指定最终的输出数据的kv类型  ，有时候不需要reduce过程，如果有的话最终输出指的就是指reducekv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // 指定job的文件输入的原始目录
        //paths指你的待处理文件可以在多个目录里边
        //第一个参数是你给那个job设置  后边的参数 逗号分隔的多个路径 路径是在hdfs里的
        FileInputFormat.addInputPath(job, input);
        // 指定job 的输出结果所在的目录
        FileOutputFormat.setOutputPath(job, output);
        /*
         * 找yarn通信
         * 将job中配置的参数，  以及job所用的java类所在的jar包提交给yarn去运行
         */
        /*job.submit();*/
        // 参数表示程序执行完，告诉我们是否执行成功
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    /*
     * KEYIN:默认情况下，是mr框架所读到的一行文本的起始偏移量，类型：long
     * 但是在Hadoop中有更精简的序列化接口，所以不直接用long ，而是用Longwriterable
     * VALUEIN：默认情况下，是mr框架所读的一行文本的内容， 类型：String  ,同上，用Text(import org.apache.hadoop.io.Text)
     *
     * KEYOUT:    是用户自定义逻辑处理完成之后输出数据的key,在此处是单词，类型 String  同上，用Text
     * VALUEOUT:  是用户自定义逻辑处理完成之后输出数据的value,在此处是单词次数，类型 Integer 同上，用 Intwriterable
     */


	/*public class WordcountMapper  extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>{

	}*/

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        //实质是包含一个整数的对象
        private final static IntWritable one = new IntWritable(1);
        //其实质是一个包含byte[] bytes的对象，将传入的字符串转换为byte[]
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //默认将传入的字符串根据" \t\n\r\f"分割，不是真分割，只是记录下了需要分割的位置
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                //根据之前做好的标记，截取字符串
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    /*
     * KEYIN, VALUEIN, 对应mapper的KEYOUT,VALUEOUT  类型对应
     *
     * KEYOUT, VALUEOUT,是自定义reduce逻辑处理结果的输出数据类型
     *
     * KEYOUT是单词
     * VALUEOUT是总次数
     */
        /*public class WordcountReduce extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>{
    }*/

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        //将相同key的value累加
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
}