package airsort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 每年每月3个温度最高时刻，并降序排列
 */
public class AirSort {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, AirSort.class.getSimpleName());
        job.setJarByClass(AirSort.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(Job.getInstance(conf), new Path("/airsort/in"));

        job.setMapperClass(M.class);
        job.setMapOutputKeyClass(AirBean.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setGroupingComparatorClass(G.class);

        job.setPartitionerClass(P.class);

        job.setReducerClass(R.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path("/airsort/out"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class AirBean implements WritableComparable {
    private int year;
    private int mouth;
    private Double air;

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getMouth() {
        return mouth;
    }

    public void setMouth(int mouth) {
        this.mouth = mouth;
    }

    public Double getAir() {
        return air;
    }

    public void setAir(Double air) {
        this.air = air;
    }

    @Override
    public String toString() {
        return year + "-" + mouth + " " + air;
    }

    @Override
    public int compareTo(Object o) {
        return this == o ? 0 : -1;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(year);
        dataOutput.writeInt(mouth);
        dataOutput.writeDouble(air);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.year = dataInput.readInt();
        this.mouth = dataInput.readInt();
        this.air = dataInput.readDouble();

    }


}

class M extends Mapper<LongWritable, Text, AirBean, DoubleWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(" ");
        String date = line[0];
        String[] str = date.split("-");
        String year = str[0];
        String mouth = str[1];
        Double air = Double.parseDouble(line[1]);
        AirBean airBean = new AirBean();
        airBean.setYear(Integer.parseInt(year));
        airBean.setMouth(Integer.parseInt(mouth));
        airBean.setAir(air);
        context.write(airBean, new DoubleWritable(air));
    }
}

/**
 * 1.继承WritableComparator
 * 2.调用父类的有参数构造
 * 3.指定分组规则（重写方法）
 */
class G extends WritableComparator {
    public G() {
        super(AirBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        //3.1对形参进行强制类型转换
        AirBean first = (AirBean) a;
        AirBean second = (AirBean) b;
        //3.2指定分组规则
        int result = Integer.compare(first.getYear(), second.getYear());
        int M = Integer.compare(first.getMouth(), second.getMouth());
        return result == 0 ? M : result;
    }
}

class S extends WritableComparator {

}

class P extends HashPartitioner<AirBean, DoubleWritable> {
    @Override
    public int getPartition(AirBean key, DoubleWritable value, int numReduceTasks) {
        return key.hashCode() % numReduceTasks;
    }
}

class R extends Reducer<AirBean, DoubleWritable, Text, NullWritable> {
    @Override   
    protected void reduce(AirBean key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

    }
}