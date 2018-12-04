package com.mapreduce.commonfriends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CommonFriendsStepOne {
    public static class CommonFriendsStepOneMapper extends Mapper<LongWritable, Text, Text, Text> {

        Text k = new Text();
        Text v = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] splits = line.split(":");
            String person = splits[0];
            String[] friends = splits[1].split(",");

            for (String friend : friends) {
                k.set(friend);
                v.set(person);
                context.write(k,v);
            }
        }
    }

    public static class CommonFriendsStepOneReducer extends Reducer<Text,Text,Text,Text> {
        @Override
        protected void reduce(Text friend, Iterable<Text> persons, Context context) throws IOException, InterruptedException {
            StringBuffer buffer = new StringBuffer();

            for (Text ptext : persons) {
                buffer.append(ptext).append("-");
            }

            context.write(friend,new Text(buffer.toString()));
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(CommonFriendsStepOne.class);

        //告诉程序，我们的程序所用的mapper类和reducer类是什么
        job.setMapperClass(CommonFriendsStepOneMapper.class);
        job.setReducerClass(CommonFriendsStepOneReducer.class);

        //告诉框架，我们程序输出的数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //这里可以进行combiner组件的设置
//        job.setCombinerClass(IndexStepOne.IndexStepOneReducer.class);


        //告诉框架，我们要处理的数据文件在那个路劲下
        FileInputFormat.setInputPaths(job, new Path("/common/input"));

        //告诉框架，我们的处理结果要输出到什么地方
        FileOutputFormat.setOutputPath(job, new Path("/common/output"));

        boolean res = job.waitForCompletion(true);

        System.exit(res?0:1);
    }

}
