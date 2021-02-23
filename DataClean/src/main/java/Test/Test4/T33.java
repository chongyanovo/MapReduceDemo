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
import java.util.ArrayList;

public class T33 {
    public static Path INPUT_PATH = new Path("/Volumes/software/IdeaProjects/DataClean/src/test/mapreduce/Test4/T3/input");
    public static Path OUTPUT_PATH = new Path("/Volumes/software/IdeaProjects/DataClean/src/test/mapreduce/Test4/T3/output");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "T3");
        job.setJarByClass(T33.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, INPUT_PATH);

        job.setMapperClass(M33.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DataBean33.class);

        job.setReducerClass(R33.class);
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

class M33 extends Mapper<LongWritable, Text, Text, DataBean33> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");
        DataBean33 dataBean33 = new DataBean33();
        dataBean33.setSex(line[2]);
        dataBean33.setName(line[3]);
        context.write(new Text(line[0]), dataBean33);
    }
}

class R33 extends Reducer<Text, DataBean33, Text, NullWritable> {
    ArrayList<String> list = new ArrayList<String>();

    @Override
    protected void reduce(Text key, Iterable<DataBean33> values, Context context) throws IOException, InterruptedException {
        int countMan = 0;
        int countWoman = 0;
        for (DataBean33 i : values) {
            String sex = i.getSex();
            if (sex.equals("男")) {
                countMan++;
            } else {
                countWoman++;
            }
        }
        list.add(key.toString()+"\t"+"男"+"\t"+String.valueOf(countMan));
        list.add(key.toString()+"\t"+"女"+"\t"+String.valueOf(countWoman));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (String i:list){
            context.write(new Text(i), NullWritable.get());
        }
    }
}

class DataBean33 implements Writable {
    private String sex;
    private String name;

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(sex);
        dataOutput.writeUTF(name);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.sex = dataInput.readUTF();
        this.name = dataInput.readUTF();
    }
}
