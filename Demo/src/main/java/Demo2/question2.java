package Demo2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.TreeMap;

public class question2 {
    private static Path INPATH = new Path("hdfs://localhost:9000/demo2/out1_2");
    private static Path OUTPATH = new Path("hdfs://localhost:9000/demo2/out2");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(question2.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, INPATH);

        job.setMapperClass(mapper2.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Bean.class);

        job.setReducerClass(reducer2.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Bean.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, OUTPATH);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

//2、按商家进行分区（每个商家对应一个文件夹），按销售额从高到低的顺序输出商品信息
class mapper2 extends Mapper<LongWritable, Text, Text, Bean> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] lines = value.toString().trim().split(",");
        Bean bean = new Bean();
        bean.setGood(lines[2]);
        int sum = Integer.parseInt(lines[3]) + Integer.parseInt(lines[5]);
        bean.setSales(sum);
        String shop = lines[1];
        context.write(new Text(shop), bean);
    }
}

class reducer2 extends Reducer<Text, Bean, Text, Bean> {
    private MultipleOutputs<Text, Bean> mos = null;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        mos = new MultipleOutputs<>(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Bean> values, Context context) throws IOException, InterruptedException {
        for (Bean i : values) {
            mos.write(key, i, key + "/");
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        mos.close();
        mos = null;
    }
}

class Bean implements WritableComparable<Bean> {
    private String good;
    private int sales;

    public String getGood() {
        return good;
    }

    public void setGood(String good) {
        this.good = good;
    }

    public int getSales() {
        return sales;
    }

    public void setSales(int sales) {
        this.sales = sales;
    }

    @Override
    public String toString() {
        return good + "\t" + sales;
    }

    @Override
    public int compareTo(Bean o) {
        int result = this.good.compareTo(o.getGood());
        if (result == 0) {
            result = o.getSales() - this.sales;
        }
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(good);
        dataOutput.writeInt(sales);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.good = dataInput.readUTF();
        this.sales = dataInput.readInt();
    }
}
