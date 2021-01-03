package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.example.inputFormat.PersonInputFormat;
import org.example.outPutFormat.PersonOutPutFormat;

import java.io.IOException;


/**
 * @version : 1.0
 * @description: 自定义输出格式
 * @author: tianwen
 * @create: 2021/1/2 13:11
 **/
public class PersonCountOutPutFormat {

    /**
     * map
     */
    public static class WordMapper extends Mapper<Object, PersonInputFormat.PersonWriteAble, PersonInputFormat.PersonWriteAble, IntWritable> {
        @Override
        protected void map(Object key, PersonInputFormat.PersonWriteAble value, Context context) throws IOException, InterruptedException {
            context.write(value, new IntWritable(1));
        }
    }

    /**
     * reduce
     */
    public static class WordReducer extends Reducer<PersonInputFormat.PersonWriteAble, IntWritable, PersonInputFormat.PersonWriteAble, IntWritable> {
        @Override
        protected void reduce(PersonInputFormat.PersonWriteAble key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum = sum + value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "person count");

        job.setInputFormatClass(PersonInputFormat.class);
        job.setJarByClass(PersonCountOutPutFormat.class);


        job.setMapperClass(PersonCountOutPutFormat.WordMapper.class);
        job.setReducerClass(PersonCountOutPutFormat.WordReducer.class);

        job.setOutputKeyClass(PersonInputFormat.PersonWriteAble.class);
        job.setOutputValueClass(IntWritable.class);


        job.setOutputFormatClass(PersonOutPutFormat.class);

//        FileInputFormat.addInputPath(job, new Path(args[0]));

//        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
