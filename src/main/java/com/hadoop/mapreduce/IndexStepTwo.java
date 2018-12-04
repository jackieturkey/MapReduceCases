package com.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class IndexStepTwo {

    public static class IndexStepTwoMapper extends Mapper<IntWritable, Text,Text,Text> {


        Text k = new Text();
        Text v = new Text();

        @Override
        protected void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t");
            String word_field = fields[0];
            String count = fields[1];
            String[] split = word_field.split("--");

            String word = split[0];
            String file = split[1];

            k.set(word);
            v.set(file+"--"+count);

            context.write(k,v);

        }
    }

    public static class IndexStepTwoReducer extends Reducer<Text,Text,Text,Text>{

        Text v = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer buffer = new StringBuffer();

            for (Text value:values) {
                buffer.append(value.toString()).append(" ");
            }
            context.write(key,v);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(IndexStepOne.class);

        //告诉程序，我们的程序所用的mapper类和reducer类是什么
        job.setMapperClass(IndexStepTwoMapper.class);
        job.setReducerClass(IndexStepTwoReducer.class);

        //告诉框架，我们程序输出的数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //这里可以进行combiner组件的设置
//        job.setCombinerClass(IndexStepTwoReducer.class);

        //告诉框架，我们程序使用的数据读取组件 结果输出所用的组件是什么
        //TextInputFormat是mapreduce程序中内置的一种读取数据组件  准确的说 叫做 读取文本文件的输入组件
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //告诉框架，我们要处理的数据文件在那个路劲下
        FileInputFormat.setInputPaths(job, new Path("/index/input"));

        //告诉框架，我们的处理结果要输出到什么地方
        FileOutputFormat.setOutputPath(job, new Path("/index/output2"));

        boolean res = job.waitForCompletion(true);

        System.exit(res?0:1);
    }

}
