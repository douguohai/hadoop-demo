package org.example.outPutFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.*;
import org.example.inputFormat.PersonInputFormat;
import org.example.writeAble.PersonWriteAble;

import java.io.IOException;

/**
 * @version : 1.0
 * @description: java类作用描述
 * @author: tianwen
 * @create: 2021/1/3 13:32
 **/
public class PersonOutPutFormat extends OutputFormat<PersonWriteAble, IntWritable> {

    @Override
    public RecordWriter<PersonWriteAble, IntWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new RecordWriter<PersonWriteAble, IntWritable>() {

            @Override
            public void write(PersonWriteAble personWriteAble, IntWritable intWritable) throws IOException, InterruptedException {

                System.out.println("开始处理结果");
                System.out.println("key: " + personWriteAble + " value:" + intWritable);
            }

            @Override
            public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
                System.out.println("close");
            }
        };
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {

    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new OutputCommitter() {

            @Override
            public void setupJob(JobContext jobContext) throws IOException {

            }

            @Override
            public void setupTask(TaskAttemptContext taskAttemptContext) throws IOException {

            }

            @Override
            public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) throws IOException {
                return false;
            }

            @Override
            public void commitTask(TaskAttemptContext taskAttemptContext) throws IOException {

            }

            @Override
            public void abortTask(TaskAttemptContext taskAttemptContext) throws IOException {

            }
        };
    }
}
