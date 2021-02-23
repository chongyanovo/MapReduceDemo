package WenduSort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartition extends Partitioner<Bean, Text> {

    @Override
    public int getPartition(Bean bean, Text text, int i) {
        return (bean.getYear() - 1949) % i;
    }
}
