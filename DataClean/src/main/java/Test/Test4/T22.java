package Test.Test4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.TreeMap;

public class T22 {
    public static Path INPUT_PATH2 = new Path("/Volumes/software/IdeaProjects/DataClean/src/test/mapreduce/Test/Test4/T2/input");
    public static Path OUTPUT_PATH2 = new Path("/Volumes/software/IdeaProjects/DataClean/src/test/mapreduce/Test/Test4/T2/output");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "T22");
        job.setJarByClass(T22.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, INPUT_PATH2);

        job.setMapperClass(M22.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DataBean22.class);

        //job.setPartitionerClass(P22.class);

        job.setReducerClass(R22.class);
        job.setOutputKeyClass(DataBean22.class);
        job.setOutputValueClass(NullWritable.class);
        //job.setNumReduceTasks(2);

        FileSystem fileSystem = OUTPUT_PATH2.getFileSystem(conf);
        if (fileSystem.exists(OUTPUT_PATH2)) {
            fileSystem.delete(OUTPUT_PATH2, true);
        }

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, OUTPUT_PATH2);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class M22 extends Mapper<LongWritable, Text, Text, DataBean22> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");
        DataBean22 dataBean22 = new DataBean22();
        dataBean22.setArea(line[0]);
        dataBean22.setName(line[3]);
        dataBean22.setAge(Integer.parseInt(line[1]));
        dataBean22.setSex(line[2]);
        context.write(new Text(line[2]), dataBean22);
    }
}

class P22 extends Partitioner<Text, DataBean22> {
    @Override
    public int getPartition(Text text, DataBean22 dataBean22, int i) {
        String sex = dataBean22.getSex();
        if (sex.equals("男")) {
            return 0;
        } else {
            return 1;
        }
    }
}

class R22 extends Reducer<Text, DataBean22, DataBean22, NullWritable> {
    //HashMap<DataBean22, Integer> map = new HashMap<DataBean22, Integer>();
    private TreeMap<Integer, DataBean22> mapMan = new TreeMap<Integer, DataBean22>();
    private TreeMap<Integer, DataBean22> mapWoman = new TreeMap<Integer, DataBean22>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

    }

    @Override
    protected void reduce(Text key, Iterable<DataBean22> values, Context context) throws IOException, InterruptedException {
        for (DataBean22 i : values) {
            if (i.getSex().equals("男")) {
                mapMan.put(i.getAge(), i);
            } else {
                mapWoman.put(i.getAge(), i);
            }
            //context.write(i, NullWritable.get());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        while (mapMan.size() > 1) {
            mapMan.remove(mapMan.firstKey());
        }
        while (mapWoman.size() > 1) {
            mapWoman.remove(mapWoman.firstKey());
        }
        context.write(mapMan.get(mapMan.firstKey()), NullWritable.get());
        context.write(mapWoman.get(mapWoman.firstKey()), NullWritable.get());
    }
}

class DataBean22 implements Writable {
    private String area;
    private String name;
    private int age;
    private String sex;

    public String getArea() {
        return area;
    }

    public void setArea(String area) {
        this.area = area;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    @Override
    public String toString() {
        return area + "\t" + name + "\t" + age + "\t" + sex;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(area);
        dataOutput.writeUTF(name);
        dataOutput.writeInt(age);
        dataOutput.writeUTF(sex);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.area = dataInput.readUTF();
        this.name = dataInput.readUTF();
        this.age = dataInput.readInt();
        this.sex = dataInput.readUTF();
    }

}