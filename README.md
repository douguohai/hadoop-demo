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
 org.example.inputFormat.MyInputFormat

package org.example.inputFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

/**
 * @version : 1.0
 * @description: 自定义hadoop输入MyInputFormat 如何使用观看 PersonCount
 * @author: tianwen
 * @create: 2021/1/2 15:14
 **/
public class MyInputFormat extends InputFormat<LongWritable, MyInputFormat.Person> {


    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
        //在此处计算总数，计划切片数据
        List<InputSplit> splits = new ArrayList<InputSplit>();
        int start = 0;
        int count = 2;
        for (int i = 0; i < 20; i++) {
            splits.add(new PersonInputSplit(start, start + count));
            start = start + i * count;
        }
        return splits;
    }


    @Override
    public RecordReader<LongWritable, Person> createRecordReader(final InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        //根据上面的切片，查询最总返回的数据
        return new RecordReader<LongWritable, Person>() {

            private LongWritable key;

            private Person value;

            private int index = 0;

            private Stack<Person> personStack = new Stack<Person>();

            /**
             * 要处理的切片
             */
            private PersonInputSplit personInputSplit;

            @Override
            public void initialize(InputSplit inputSplit1, TaskAttemptContext taskAttemptContext1) {
                personInputSplit = (PersonInputSplit) inputSplit1;
                System.out.println(personInputSplit);
                for (long i = personInputSplit.getStart(); i < personInputSplit.end; i++) {
                    personStack.push(new Person("张丹" + i, 12));
                    System.out.println(new Person("张丹" + i, 12).toString());
                }

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
            public Person getCurrentValue() throws IOException, InterruptedException {
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
                System.out.println("===== close =====");
            }
        };
    }


    public static class Person implements Serializable, Writable {
        private String name;
        private Integer age;

        public Person() {
        }

        public Person(String name, Integer age) {
            this.name = name;
            this.age = age;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Integer getAge() {
            return age;
        }

        public void setAge(Integer age) {
            this.age = age;
        }

        @Override
        public String toString() {
            return "Person{" +
                    "name='" + name + '\'' +
                    ", age='" + age + '\'' +
                    '}';
        }

        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(name);
            dataOutput.writeInt(age);
        }

        public void readFields(DataInput dataInput) throws IOException {
            this.name = dataInput.readUTF();
            this.age = dataInput.readInt();

        }
    }


    public static class PersonInputSplit extends InputSplit implements Writable {

        /**
         * 开始
         */
        private long start;

        /**
         * 步长
         */
        private long end;


        public PersonInputSplit() {
        }

        public PersonInputSplit(Integer start, Integer end) {
            this.start = start;
            this.end = end;
        }

        @Override
        public long getLength() throws IOException, InterruptedException {
            return start - end;
        }

        @Override
        public String[] getLocations() throws IOException, InterruptedException {
            return new String[0];
        }

        public long getStart() {
            return start;
        }

        public void setStart(Integer start) {
            this.start = start;
        }

        public long getEnd() {
            return end;
        }

        public void setEnd(Integer end) {
            this.end = end;
        }

        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeLong(start);
            dataOutput.writeLong(end);
        }

        public void readFields(DataInput dataInput) throws IOException {
            this.start = dataInput.readLong();
            this.end = dataInput.readLong();
        }

        @Override
        public String toString() {
            return "PersonInputSplit{" +
                    "start=" + start +
                    ", end=" + end +
                    '}';
        }
    }
}

```
