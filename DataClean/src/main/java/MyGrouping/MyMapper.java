package MyGrouping;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<LongWritable, Text,OrderBean,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");

        String orderId=line[0];
        Double price=Double.valueOf(line[2]);

        OrderBean orderBean = new OrderBean();
        orderBean.setOrderId(orderId);
        orderBean.setPrice(price);

        context.write(orderBean,value);
    }
}
