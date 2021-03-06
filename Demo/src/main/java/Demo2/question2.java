package Demo2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.TreeMap;

public class question2 {

}

//2、按商家进行分区（每个商家对应一个文件夹），按销售额从高到低的顺序输出商品信息
class mapper2 extends Mapper<LongWritable, Text, Bean, IntWritable> {
    TreeMap<String, Integer> map = new TreeMap<>();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] lines = value.toString().trim().split(",");
        Bean bean = new Bean();
        bean.setShop(lines[1]);
        bean.setGood(lines[2]);
        int sum = Integer.parseInt(lines[3]) + Integer.parseInt(lines[5]);
        context.write(bean, new IntWritable(sum));
    }
}

class reducer2 extends Reducer<Bean, IntWritable, Text, IntWritable> {
    private MultipleOutputs<Text, IntWritable> mos = null;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        mos = new MultipleOutputs<>(context);
    }

    @Override
    protected void reduce(Bean key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        mos.close();
        mos=null;
    }
}

class Bean implements WritableComparable<Bean> {
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
    public int compareTo(Bean o) {
        int result = this.shop.compareTo(o.getShop());
        if (result == 0) {
            result = this.good.compareTo(o.getGood());
        }
        return result;
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
}
