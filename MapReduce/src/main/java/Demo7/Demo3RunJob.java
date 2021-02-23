package Demo7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author mars
 * 任务3、根据汇总表统计供应商每个月的销售数据
 * 供应商名称   月份  销售商品数量
 * A公司       2020-09   1
 * B公司       2020-09   1
 * B公司       2020-10   5
 */
public class Demo3RunJob {
    final static Path INPUT_PATH = new Path("/Volumes/software/IdeaProjects/MapReduce/src/test/java/demo7/demo7_3/input");
    final static Path OUTPUT_PATH = new Path("/Volumes/software/IdeaProjects/MapReduce/src/test/java/demo7/demo7_3/output");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "demo7_3");
        job.setJarByClass(Demo3RunJob.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, INPUT_PATH);

        job.setMapperClass(Demo3Mapper.class);
        job.setMapOutputKeyClass(MyKey3.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(Demo3Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileSystem fileSystem = OUTPUT_PATH.getFileSystem(conf);
        if (fileSystem.exists(OUTPUT_PATH)) {
            fileSystem.delete(OUTPUT_PATH, true);
        }

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, OUTPUT_PATH);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class Demo3Mapper extends Mapper<LongWritable, Text, MyKey3, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().trim().split(",");
        String mouth = line[5].substring(0,7);
        MyKey3 myKey = new MyKey3();
        myKey.setCompare(line[10]);
        myKey.setMouth(mouth);
        context.write(myKey, new IntWritable(Integer.parseInt(line[3])));
    }
}

class Demo3Reducer extends Reducer<MyKey3, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(MyKey3 key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable i : values) {
            sum += i.get();
        }
        context.write(new Text(key.toString()), new IntWritable(sum));
    }
}

class MyKey3 implements WritableComparable<MyKey3> {
    private String compare;
    private String mouth;

    public String getCompare() {
        return compare;
    }

    public void setCompare(String compare) {
        this.compare = compare;
    }

    public String getMouth() {
        return mouth;
    }

    public void setMouth(String mouth) {
        this.mouth = mouth;
    }

    @Override
    public String toString() {
        return compare + '\t' + mouth + "\t";
    }

    @Override
    public int compareTo(MyKey3 mykey) {
        int result = this.compare.compareTo(mykey.getCompare());
        if (result == 0) {
            result = this.mouth.compareTo(mykey.getMouth());
        }
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(compare);
        dataOutput.writeUTF(mouth);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.compare = dataInput.readUTF();
        this.mouth = dataInput.readUTF();
    }
}