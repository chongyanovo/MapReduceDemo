package Demo2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.TreeMap;

public class question3 {
    private static Path INPATH = new Path("hdfs://localhost:9000/demo2/out1_2");
    private static Path OUTPATH = new Path("hdfs://localhost:9000/demo2/out3");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(question3.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, INPATH);

        job.setMapperClass(mapper3.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, OUTPATH);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

//3、找出南京市1月份销售量最高的3个商品名称，格式如下
class mapper3 extends Mapper<LongWritable, Text, Text, NullWritable> {
    int K;
    TreeMap<Integer, myBean> map = new TreeMap<>();
    ArrayList<String> list = new ArrayList<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        K = 3;
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] lines = value.toString().trim().split(",");
        myBean myBean = new myBean();
        myBean.setShop(lines[1]);
        myBean.setGood(lines[2]);
        //int sum = Integer.parseInt(lines[3]) + Integer.parseInt(lines[5]);
        map.put(Integer.parseInt(lines[3]), myBean);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Integer i : map.keySet()) {
            list.add(map.get(i).toString() + "\t" + i);
        }
        Collections.reverse(list);
        for (int i = 0; i < K; i++) {
            context.write(new Text(list.get(i)), NullWritable.get());
        }
    }
}

class myBean implements WritableComparable<myBean> {
    private String shop;
    private String good;

    public String getShop() {
        return shop;
    }

    public void setShop(String shop) {
        this.shop = shop;
    }

    public String getGood() {
        return good;
    }

    public void setGood(String good) {
        this.good = good;
    }

    @Override
    public String toString() {
        return shop + "\t" + good;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(shop);
        dataOutput.writeUTF(good);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.shop = dataInput.readUTF();
        this.good = dataInput.readUTF();
    }

    @Override
    public int compareTo(myBean o) {
        int result = this.shop.compareTo(o.getShop());
        if (result == 0) {
            result = this.good.compareTo(o.getGood());
        }
        return result;
    }
}