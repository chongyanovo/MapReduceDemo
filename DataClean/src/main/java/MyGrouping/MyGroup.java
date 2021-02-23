package MyGrouping;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyGroup extends WritableComparator {
    MyGroup() {
        super(OrderBean.class,true);
    }

//    @Override
//    public int compare(WritableComparable a, WritableComparable b) {
//        OrderBean first = (OrderBean) a;
//        OrderBean second = (OrderBean) b;
//        int result = first.getOrderId().compareTo(second.getOrderId());
//        return result;
//    }
}
