package Test.Test4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
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

public class T1 {
    public static Path INPUT_PATH = new Path("/Volumes/software/IdeaProjects/DataClean/src/test/mapreduce/Test4/T1/input");
    public static Path OUTPUT_PATH = new Path("/Volumes/software/IdeaProjects/DataClean/src/test/mapreduce/Test4/T1/output");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "T1");
        job.setJarByClass(T1.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, INPUT_PATH);

        job.setMapperClass(M1.class);
        job.setMapOutputKeyClass(Bean1.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setPartitionerClass(P1.class);

        job.setReducerClass(R1.class);
        job.setOutputKeyClass(Bean1.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(3);

        FileSystem fileSystem = OUTPUT_PATH.getFileSystem(conf);
        if (fileSystem.exists(OUTPUT_PATH)) {
            fileSystem.delete(OUTPUT_PATH, true);
        }

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, OUTPUT_PATH);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class M1 extends Mapper<LongWritable, Text, Bean1, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");
        Bean1 bean1 = new Bean1();
        bean1.setArea(line[0]);
        bean1.setName(line[3]);
        bean1.setAge(Integer.parseInt(line[1]));
        bean1.setSex(line[2]);
        context.write(bean1, NullWritable.get());
    }
}

class P1 extends Partitioner<Bean1, NullWritable> {
    @Override
    public int getPartition(Bean1 bean1, NullWritable nullWritable, int i) {
        //return (bean1.getArea().hashCode() & Integer.MAX_VALUE) % i;
        if (bean1.getArea().equals("武汉市")){
            return 0;
        }else if (bean1.getArea().equals("黄石市")){
            return 1;
        }else {
            return 2;
        }
    }
}

class R1 extends Reducer<Bean1, NullWritable, Bean1, NullWritable> {
    @Override
    protected void reduce(Bean1 key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}

class Bean1 implements WritableComparable<Bean1> {
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
    public int compareTo(Bean1 bean) {
        return bean.getAge() - this.age;
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
