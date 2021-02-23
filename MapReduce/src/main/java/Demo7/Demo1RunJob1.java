package Demo7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author mars
 * 任务1、将销售表、订单表和商品表合并为汇总表
 * 商品编号 商品类型 商品名称     商品数量 订单id   订单时间  客户名 快递公司   客户住址     客户所在省份   供应商名称
 * A113      电脑    联想笔记本A     1      B1123   2020-09-23  张三   中通       仙林大学城    江苏         A公司
 * A114      办公    铅笔A           1      B1124   2020-09-24  李四   申通       江宁          江苏         B公司
 * A115      办公    尺子B           2      B1125   2020-10-04  李四   申通       江宁          江苏         B公司
 * A116      办公    橡皮A           3      B1126   2020-11-5   李冰   顺丰       杭州          浙江         B公司
 */
public class Demo1RunJob1 {
    final static Path INPUT_PATH = new Path("/Volumes/software/IdeaProjects/MapReduce/src/test/java/demo7/demo7_1_1/input");
    final static Path INPUT_PATH1 = new Path("/Volumes/software/IdeaProjects/MapReduce/src/test/java/demo7/demo7_1_1/input/product.txt");
    final static Path INPUT_PATH2 = new Path("/Volumes/software/IdeaProjects/MapReduce/src/test/java/demo7/demo7_1_1/input/sales.txt");
    final static Path OUTPUT_PATH = new Path("/Volumes/software/IdeaProjects/MapReduce/src/test/java/demo7/demo7_1_1/output");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "demo7_1_1");
        job.setJarByClass(Demo1RunJob1.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, INPUT_PATH);

        //四个参数分别是:任务名,输入路径,输入类型,指定调用的 Mapper
        MultipleInputs.addInputPath(job, INPUT_PATH2, TextInputFormat.class, SalesMapper.class);
        MultipleInputs.addInputPath(job, INPUT_PATH1, TextInputFormat.class, ProductMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ValueBean.class);

        job.setReducerClass(Demo1Reducer1.class);
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

class SalesMapper extends Mapper<LongWritable, Text, Text, ValueBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().trim().split(",");
        //这里的str字符串是两个表要连接的共同部分，要作为key传入Reduce，利用默认分组进行拼接表
        String str = line[0] + "," + line[1] + "," + line[2];
        ValueBean valueBean = new ValueBean();
        //这个values是我自定义的JavaBean中的value属性，存放的是两个表的不同部分（即为要拼接的不同部分）
        String values = line[3] + "," + line[4];
        valueBean.setValues(values);
        valueBean.setFlag("0");
        context.write(new Text(str), valueBean);
    }
}

class ProductMapper extends Mapper<LongWritable, Text, Text, ValueBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //商品表同理进行拆分处理
        String[] line = value.toString().trim().split(",");
        String str = line[0] + "," + line[1] + "," + line[2];
        ValueBean valueBean = new ValueBean();
        String values = line[3];
        valueBean.setValues(values);
        valueBean.setFlag("1");
        context.write(new Text(str), valueBean);
    }
}

class Demo1Reducer1 extends Reducer<Text, ValueBean, Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<ValueBean> values, Context context) throws IOException, InterruptedException {
        //临时字符串str用来保持最后的拼接完成的字符串
        String str = "";
        //字符串first和字符串second用来保持分别来自不同表的数据
        String first = "";
        String second = "";
        //通过ValueBean中的flag标识，区分数据来自哪张表
        for (ValueBean i : values) {
            if (i.getFlag().equals("0")) {
                second = i.toString();
            }
            if (i.getFlag().equals("1")) {
                first = i.toString();
            }
        }
        str = key.toString() + "," + first + "," + second;
        context.write(new Text(str), NullWritable.get());
    }
}

class ValueBean implements WritableComparable<ValueBean> {
    /**
     * values属性放的是两个表的不同部分
     */
    String values;
    String flag;

    public String getValues() {
        return values;
    }

    public void setValues(String values) {
        this.values = values;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    @Override
    public String toString() {
        return values;
    }

    @Override
    public int compareTo(ValueBean valueBean) {
        int result = this.values.compareTo(valueBean.getValues());
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(values);
        dataOutput.writeUTF(flag);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.values = dataInput.readUTF();
        this.flag = dataInput.readUTF();
    }
}


