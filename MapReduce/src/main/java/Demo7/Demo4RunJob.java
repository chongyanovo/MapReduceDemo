package Demo7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
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
 * 任务4、根据汇总表统计快递公司每年的快递单数量
 * 快递公司   年份   快递单数量
 * 中通       2020    1
 * 申通       2020    2
 * 顺丰       2020    1
 */
public class Demo4RunJob {
    final static Path INPUT_PATH = new Path("/Volumes/software/IdeaProjects/MapReduce/src/test/java/demo7/demo7_4/input");
    final static Path OUTPUT_PATH = new Path("/Volumes/software/IdeaProjects/MapReduce/src/test/java/demo7/demo7_4/output");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "demo7_4");
        job.setJarByClass(Demo4RunJob.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, INPUT_PATH);

        job.setMapperClass(Demo4Mapper.class);
        job.setMapOutputKeyClass(MyKey4.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(Demo4Reducer.class);
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

class Demo4Mapper extends Mapper<LongWritable, Text, MyKey4, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().trim().split(",");
        int year = Integer.parseInt(line[5].substring(0, 4));
        MyKey4 myKey = new MyKey4();
        myKey.setCompare(line[7]);
        myKey.setYear(year);
        context.write(myKey, new IntWritable(1));
    }
}

class Demo4Reducer extends Reducer<MyKey4, IntWritable, Text, IntWritable> {
    int sum = 0;

    @Override
    protected void reduce(MyKey4 key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        for (IntWritable i : values) {
            sum += i.get();
        }
        context.write(new Text(key.toString()), new IntWritable(sum));
    }
}

class MyKey4 implements WritableComparable<MyKey4> {
    private String compare;
    private int year;

    public String getCompare() {
        return compare;
    }

    public void setCompare(String compare) {
        this.compare = compare;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    @Override
    public String toString() {
        return compare + "\t" + year;
    }

    @Override
    public int compareTo(MyKey4 mykey) {
        int result = this.compare.compareTo(mykey.getCompare());
        if (result == 0) {
            result = this.year - mykey.getYear();
        }
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(compare);
        dataOutput.writeInt(year);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.compare = dataInput.readUTF();
        this.year = dataInput.readInt();
    }
}

