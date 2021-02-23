package Test.Test2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
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

public class T1 {
    public static Path INPUT_PATH = new Path("/Volumes/software/IdeaProjects/DataClean/src/test/mapreduce/Test2/T1/input");
    public static Path OUTPUT_PATH = new Path("/Volumes/software/IdeaProjects/DataClean/src/test/mapreduce/Test2/T1/output");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "T1");
        job.setJarByClass(T1.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, INPUT_PATH);

        job.setMapperClass(M1.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Bean.class);

        job.setReducerClass(R1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, OUTPUT_PATH);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class M1 extends Mapper<LongWritable, Text, Text, Bean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(",");
        Bean bean = new Bean();
        bean.setClazz(line[2]);
        bean.setNum(Integer.parseInt(line[4]));
        context.write(new Text(line[1]), bean);
    }
}

class R1 extends Reducer<Text, Bean, Text, IntWritable> {
    HashMap<String, Integer> map = new HashMap<String, Integer>();

    @Override
    protected void reduce(Text key, Iterable<Bean> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        HashSet set = new HashSet();
        for (Bean i : values) {
            if (!set.contains(i.getClazz())) {
                sum += i.getNum();
                set.add(i.getClazz());
            }
        }
        map.put(key.toString(), sum);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (String i : map.keySet()) {
            sum += map.get(i);
        }
        context.write(new Text("人工智能学院的学生总数是:" + "\t"), new IntWritable(sum));
    }
}

class Bean implements Writable {
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

