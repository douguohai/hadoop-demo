package org.example.writeAble;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @version : 1.0
 * @description: java类作用描述
 * @author: tianwen
 * @create: 2021/1/3 14:17
 **/
@Data
@EqualsAndHashCode
@ToString
public class PersonWriteAble implements WritableComparable<PersonWriteAble> {

    private Text name;
    private IntWritable age;

    public PersonWriteAble() {
        this(new Text(),new IntWritable());
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
