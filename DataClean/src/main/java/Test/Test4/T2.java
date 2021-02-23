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
import java.util.ArrayList;
import java.util.TreeMap;

public class T2 {
    public static Path INPUT_PATH = new Path("/Volumes/software/IdeaProjects/DataClean/src/test/mapreduce/Test4/T2/input");
    public static Path OUTPUT_PATH = new Path("/Volumes/software/IdeaProjects/DataClean/src/test/mapreduce/Test4/T2/output");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "T2");
        job.setJarByClass(T2.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, INPUT_PATH);

        job.setMapperClass(M2.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Bean2.class);

//        job.setPartitionerClass(P2.class);

        job.setReducerClass(R2.class);
        job.setOutputKeyClass(Bean2.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(2);

        FileSystem fileSystem = OUTPUT_PATH.getFileSystem(conf);
        if (fileSystem.exists(OUTPUT_PATH)) {
            fileSystem.delete(OUTPUT_PATH, true);
        }

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, OUTPUT_PATH);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class M2 extends Mapper<LongWritable, Text, Text, Bean2> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");
        Bean2 bean2 = new Bean2();
        bean2.setArea(line[0]);
        bean2.setName(line[3]);
        bean2.setAge(Integer.parseInt(line[1]));
        bean2.setSex(line[2]);
        context.write(new Text(line[2]), bean2);
    }
}

//class P2 extends Partitioner<Text, Bean2> {
//    @Override
//    public int getPartition(Text text, Bean2 bean2, int i) {
//        String sex = bean2.getSex();
//        if (sex.equals("男")) {
//            return 0;
//        } else {
//            return 1;
//        }
//    }
//}

class R2 extends Reducer<Text, Bean2, Bean2, NullWritable> {
    int K = 1;
    //HashMap<Integer, Bean2> tmp = new HashMap<Integer, Bean2>();
    //ArrayList<Bean2> tmp = new ArrayList<Bean2>();

    @Override
    protected void reduce(Text key, Iterable<Bean2> values, Context context) throws IOException, InterruptedException {
        TreeMap<Integer, Bean2> map = new TreeMap<Integer, Bean2>();
        for (Bean2 i : values) {
            map.put(i.getAge(), i);
            if (map.size() > K) {
                map.remove(map.firstKey());
            }
        }
        for (Integer j : map.keySet()) {
            //tmp.add(map.get(j));
            context.write(map.get(j),NullWritable.get());
        }
    }

//    @Override
//    protected void cleanup(Context context) throws IOException, InterruptedException {
//        for (Bean2 i : tmp) {
//            context.write(i, NullWritable.get());
//        }
//    }

}

class Bean2 implements Writable {
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
