package Test.Test2;

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
import java.util.HashMap;
import java.util.HashSet;

public class T3 {
    public static Path INPUT_PATH = new Path("/Volumes/software/IdeaProjects/DataClean/src/test/mapreduce/Test2/T3/input");
    public static Path OUTPUT_PATH = new Path("/Volumes/software/IdeaProjects/DataClean/src/test/mapreduce/Test2/T3/output");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "T3");
        job.setJarByClass(T3.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, INPUT_PATH);

        job.setMapperClass(M3.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Bean3.class);

        job.setReducerClass(R3.class);
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

class M3 extends Mapper<LongWritable, Text, Text, Bean3> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(",");
        Bean3 bean3 = new Bean3();
        bean3.setClazz(line[2]);
        bean3.setNum(Integer.parseInt(line[4]));
        context.write(new Text(line[1]), bean3);
    }
}

class R3 extends Reducer<Text, Bean3, Text, NullWritable> {
    HashMap<String, Integer> map = new HashMap<String, Integer>();
    int Num = 0;

    @Override
    protected void reduce(Text key, Iterable<Bean3> values, Context context) throws IOException, InterruptedException {
        Num++;
        int clazzNum = 0;
        HashSet set = new HashSet();
        for (Bean3 i : values) {
            if (!set.contains(i.getClazz())) {
                clazzNum++;
                set.add(i.getClazz());
            }
        }
        map.put(key.toString(), clazzNum);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        String str = "人工智能学院有 " + String.valueOf(Num) + " 个专业";
        context.write(new Text(str), NullWritable.get());
        for (String i : map.keySet()) {
            String str1 = i + "专业有 " + String.valueOf(map.get(i)) + " 个班";
            context.write(new Text(str1), NullWritable.get());
        }
    }
}

class Bean3 implements Writable {
    private String clazz;
    private int num;

    public String getClazz() {
        return clazz;
    }

    public void setClazz(String clazz) {
        this.clazz = clazz;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    @Override
    public String toString() {
        return clazz + '\t' + num;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(clazz);
        dataOutput.writeInt(num);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.clazz = dataInput.readUTF();
        this.num = dataInput.readInt();
    }
}

