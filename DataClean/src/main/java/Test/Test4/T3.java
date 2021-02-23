package Test.Test4;

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

public class T3 {
    public static Path INPUT_PATH = new Path("/Volumes/software/IdeaProjects/DataClean/src/test/mapreduce/Test4/T3/input");
    public static Path OUTPUT_PATH = new Path("/Volumes/software/IdeaProjects/DataClean/src/test/mapreduce/Test4/T3/output");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "T3");
        job.setJarByClass(T3.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, INPUT_PATH);

        job.setMapperClass(M3.class);
        job.setMapOutputKeyClass(Bean3.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(R3.class);
        job.setOutputKeyClass(Bean3.class);
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

class M3 extends Mapper<LongWritable, Text, Bean3, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");
        Bean3 bean3 = new Bean3();
        bean3.setArea(line[0]);
        bean3.setSex(line[2]);
        context.write(bean3, new Text(line[3]));
    }
}

class R3 extends Reducer<Bean3, Text, Bean3, IntWritable> {
    @Override
    protected void reduce(Bean3 key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (Text i : values) {
            count++;
        }
        context.write(key, new IntWritable(count));
    }
}

class Bean3 implements Writable {
    private String area;
    private String sex;

    public String getArea() {
        return area;
    }

    public void setArea(String area) {
        this.area = area;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    @Override
    public String toString() {
        return area + "\t" + sex;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(area);
        dataOutput.writeUTF(sex);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.area = dataInput.readUTF();
        this.sex = dataInput.readUTF();
    }
}
