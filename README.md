##### hadoop wordcount 项目测试

#### 执行环境
```$xslt
maven package
docker pull registry.cn-beijing.aliyuncs.com/douguohai/hadoop2.9.2-local-single:v2
docker run -itd  --name=hadoop registry.cn-beijing.aliyuncs.com/douguohai/hadoop2.9.2-local-single:v2
docker exec -it 容器id  /bin/bash
bin/hadoop jar /root/hadoop.jar  input output 
```

#### 简单的wordcount
```$xslt
package org.example;

import cn.hutool.core.util.StrUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.List;


/**
 * @version : 1.0
 * @description: mapper reducers wordcounter
 * @author: tianwen
 * @create: 2021/1/2 13:11
 **/
public class WordCount {

    /**
     * map
     */
    public static class WordMapper extends Mapper<Object, Text, Text, IntWritable> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String wordStr = value.toString();
            List<String> words = StrUtil.splitTrim(wordStr, "。");
            for (String word : words) {
                context.write(new Text(word), new IntWritable(1));
            }

        }
    }

    /**
     * reduce
     */
    public static class WordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum = sum + value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordMapper.class);
        job.setReducerClass(WordReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

```


#### 实现了自定义的文件读取类
```$xslt
 package org.example.inputFormat;
 
 import lombok.Data;
 import lombok.EqualsAndHashCode;
 import lombok.ToString;
 import org.apache.hadoop.io.*;
 import org.apache.hadoop.mapreduce.*;
 
 import java.io.DataInput;
 import java.io.DataOutput;
 import java.io.IOException;
 import java.util.ArrayList;
 import java.util.List;
 import java.util.concurrent.BlockingDeque;
 import java.util.concurrent.LinkedBlockingDeque;
 
 /**
  * @version : 1.0
  * @description: 自定义hadoop输入 PersonInputFormat 如何使用观看 PersonCount
  * @author: tianwen
  * @create: 2021/1/2 15:14
  **/
 public class PersonInputFormat extends InputFormat<LongWritable, PersonInputFormat.PersonWriteAble> {
 
 
     @Override
     public List<InputSplit> getSplits(JobContext jobContext) {
         //在此处计算总数，计划切片数据
         List<InputSplit> splits = new ArrayList<InputSplit>();
         int start = 0;
         int count = 2;
         for (int i = 0; i < 20; i++) {
             splits.add(new PersonInputSplit(start, start + count));
             start = start + 3;
         }
         return splits;
     }
 
     @Override
     public RecordReader<LongWritable, PersonWriteAble> createRecordReader(final InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
         //根据上面的切片，查询最总返回的数据
         return new RecordReader<LongWritable, PersonWriteAble>() {
 
             private LongWritable key;
 
             private PersonWriteAble value;
 
             private int index = 0;
 
             private BlockingDeque<PersonWriteAble> personStack = new LinkedBlockingDeque<PersonWriteAble>();
 
             @Override
             public void initialize(InputSplit inputSplit1, TaskAttemptContext taskAttemptContext1) {
                 PersonInputSplit personInputSplit = (PersonInputSplit) inputSplit1;
                 System.out.println("personInputSplit:" + personInputSplit + " personInputSplit length:" + personInputSplit.getLength());
                 for (long i = personInputSplit.getStart(); i < personInputSplit.end; i++) {
                     personStack.push(new PersonWriteAble(new Text("张丹" + i), new IntWritable(12)));
                     System.out.println(new PersonWriteAble(new Text("张丹" + i), new IntWritable(12)).toString());
                 }
                 System.out.println("[personStack 大小为]" + personStack.size());
             }
 
             @Override
             public boolean nextKeyValue() throws IOException, InterruptedException {
                 //继续加载数据
                 //判断是否仍然有数据
                 if (!personStack.isEmpty()) {
                     //有数据取出数据
                     this.key = new LongWritable(index);
                     this.value = personStack.pop();
                     index = index + 1;
                     return true;
                 }
                 return false;
             }
 
             @Override
             public LongWritable getCurrentKey() throws IOException, InterruptedException {
                 return this.key;
             }
 
             @Override
             public PersonWriteAble getCurrentValue() throws IOException, InterruptedException {
                 return this.value;
             }
 
             @Override
             public float getProgress() throws IOException, InterruptedException {
                 if (personStack.size() == 0) {
                     return 1;
                 }
                 return (float) (index / personStack.size());
             }
 
             @Override
             public void close() throws IOException {
                 personStack.clear();
             }
         };
     }
 
 
     @Data
     @EqualsAndHashCode
     @ToString
     public static class PersonWriteAble implements WritableComparable<PersonWriteAble> {
         private Text name;
         private IntWritable age;
 
         public PersonWriteAble() {
         }
 
         public PersonWriteAble(Text name, IntWritable age) {
             this.name = name;
             this.age = age;
         }
 
         @Override
         public void write(DataOutput dataOutput) throws IOException {
             name.write(dataOutput);
             age.write(dataOutput);
         }
 
         @Override
         public void readFields(DataInput dataInput) throws IOException {
             name.readFields(dataInput);
             age.readFields(dataInput);
 
         }
 
         @Override
         public int compareTo(PersonWriteAble o) {
             int a = name.compareTo(o.getName());
             if (a != 0) {
                 return a;
             }
             return age.compareTo(o.getAge());
         }
     }
 
     @Data
     @ToString
     public static class PersonInputSplit extends InputSplit implements Writable {
 
         /**
          * 开始
          */
         private long start;
 
         /**
          * 结束
          */
         private long end;
 
 
         public PersonInputSplit() {
         }
 
         public PersonInputSplit(int start, int end) {
             this.start = start;
             this.end = end;
             System.out.println("初始化：" + start + " " + end);
         }
 
         @Override
         public long getLength() {
             return end - start;
         }
 
         @Override
         public String[] getLocations() {
             return new String[0];
         }
 
 
         @Override
         public void write(DataOutput dataOutput) throws IOException {
             dataOutput.writeLong(start);
             dataOutput.writeLong(end);
             System.out.println("write：" + start + " " + end);
         }
 
         @Override
         public void readFields(DataInput dataInput) throws IOException {
             this.start = dataInput.readLong();
             this.end = dataInput.readLong();
 
             if (this.start > this.end) {
                 long temp;
                 temp = start;
                 start = end;
                 end = temp;
             }
             System.out.println("readFields：" + start + " " + end);
         }
 
     }
 }

```
