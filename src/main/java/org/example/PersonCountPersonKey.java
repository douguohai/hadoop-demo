package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.example.inputFormat.PersonInputFormat;
import org.example.writeAble.PersonWriteAble;

import java.io.IOException;


/**
 * @version : 1.0
 * @description: mapper reducers wordcounter
 * @author: tianwen
 * @create: 2021/1/2 13:11
 **/
public class PersonCountPersonKey {

    /**
     * map
     */
    public static class WordMapper extends Mapper<Object, PersonWriteAble, PersonWriteAble, IntWritable> {
        @Override
        protected void map(Object key, PersonWriteAble value, Context context) throws IOException, InterruptedException {
            context.write(value, new IntWritable(1));
        }
    }

    /**
     * reduce
     */
    public static class WordReducer extends Reducer<PersonWriteAble, IntWritable, PersonWriteAble, IntWritable> {
        @Override
        protected void reduce(PersonWriteAble key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
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
        job.setJarByClass(PersonCountPersonKey.class);


        job.setMapperClass(PersonCountPersonKey.WordMapper.class);
        job.setReducerClass(PersonCountPersonKey.WordReducer.class);

        job.setOutputKeyClass(PersonWriteAble.class);
        job.setOutputValueClass(IntWritable.class);

//        FileInputFormat.addInputPath(job, new Path(args[0]));

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
