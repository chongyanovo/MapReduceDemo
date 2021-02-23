package Demo6;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class MyKey implements WritableComparable<MyKey> {
    private String type;
    private String url;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    @Override
    public String toString() {
        return type + "\t" + url + "\t";
    }

    @Override
    public int compareTo(MyKey myKey) {
        int result = myKey.getType().compareTo(this.type);
        if (result == 0) {
            result = myKey.getUrl().compareTo(this.url);
        }
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(type);
        dataOutput.writeUTF(url);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.type = dataInput.readUTF();
        this.url = dataInput.readUTF();
    }
}
