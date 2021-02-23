package Test7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Q1_1 {
    public static Path INPUT_PATH1 = new Path("hdfs://localhost:9000/Test7/Q1/input/sales.txt");
    public static Path INPUT_PATH2 = new Path("hdfs://localhost:9000/Test7/Q1/input/order.txt");
    public static Path OUTPUT_PATH = new Path("hdfs://localhost:9000/Test7/Q1/output1");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Q1_1.class);

        MultipleInputs.addInputPath(job, INPUT_PATH1, TextInputFormat.class, SalesMap.class);
        MultipleInputs.addInputPath(job, INPUT_PATH2, TextInputFormat.class, OrderMap.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ValueBean.class);

        job.setReducerClass(R1_1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileSystem fileSystem = OUTPUT_PATH.getFileSystem(conf);
        if (fileSystem.exists(OUTPUT_PATH)) {
            fileSystem.delete(OUTPUT_PATH, true);
        }

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, OUTPUT_PATH);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class SalesMap extends Mapper<LongWritable, Text, Text, ValueBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().trim().split(",");
        String str = line[4];
        ValueBean valueBean = new ValueBean();
        valueBean.setValue(line[0] + "\t" + line[1] + "\t" + line[2] + "\t" + line[3]);
        valueBean.setFlag(0);
        context.write(new Text(str), valueBean);
    }
}

class OrderMap extends Mapper<LongWritable, Text, Text, ValueBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().trim().split(",");
        String str = line[0];
        ValueBean valueBean = new ValueBean();
        valueBean.setValue(line[1] + "\t" + line[2] + "\t" + line[3] + "\t" + line[4] + "\t" + line[5]);
        valueBean.setFlag(1);
        context.write(new Text(str), valueBean);
    }
}

class R1_1 extends Reducer<Text, ValueBean, Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<ValueBean> values, Context context) throws IOException, InterruptedException {
        String str = "";
        String first = "";
        String second = "";
        for (ValueBean i : values) {
            if (i.getFlag() == 0) {
                first = i.toString();
            }
            if (i.getFlag() == 1) {
                second = i.toString();
            }
        }
        str = first + "\t" + key.toString() + "\t" + second;
        context.write(new Text(str), NullWritable.get());
    }
}

class ValueBean implements WritableComparable<ValueBean> {
    private String value;
    private int flag;

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    @Override
    public String toString() {
        return value;
    }

    @Override
    public int compareTo(ValueBean valueBean) {
        int result = this.value.compareTo(valueBean.getValue());
        if (result == 0) {
            result = this.flag - valueBean.getFlag();
        }
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(value);
        dataOutput.writeInt(flag);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.value = dataInput.readUTF();
        this.flag = dataInput.readInt();
    }
}
