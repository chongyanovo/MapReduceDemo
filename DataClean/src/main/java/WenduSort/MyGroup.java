package WenduSort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyGroup extends WritableComparator {
    MyGroup() {
        super(Bean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Bean first = (Bean) a;
        Bean second = (Bean) b;
        int result = first.getYear() - second.getYear();
        return result;
    }
}
