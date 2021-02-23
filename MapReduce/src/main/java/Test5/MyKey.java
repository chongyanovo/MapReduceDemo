package Test5;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class MyKey implements WritableComparable<MyKey> {
    private String collage;
    private String clazz;
    private int boy;
    private int girl;

    public String getCollage() {
        return collage;
    }

    public void setCollage(String collage) {
        this.collage = collage;
    }

    public String getClazz() {
        return clazz;
    }

    public void setClazz(String clazz) {
        this.clazz = clazz;
    }

    public int getBoy() {
        return boy;
    }

    public void setBoy(int boy) {
        this.boy = boy;
    }

    public int getGirl() {
        return girl;
    }

    public void setGirl(int girl) {
        this.girl = girl;
    }

    @Override
    public String toString() {
        return collage + '\t' + clazz + '\t' + boy + '\t' + girl;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(collage);
        dataOutput.writeUTF(clazz);
        dataOutput.writeInt(boy);
        dataOutput.writeInt(girl);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.collage = dataInput.readUTF();
        this.clazz = dataInput.readUTF();
        this.boy = dataInput.readInt();
        this.girl = dataInput.readInt();
    }

    @Override
    public int compareTo(MyKey myKey) {
        int result = this.collage.compareTo(myKey.getCollage());
        if (result == 0) {
            result = this.clazz.compareTo(myKey.getClazz());
            if (result == 0) {
                result = this.boy - myKey.getBoy();
                if (result == 0) {
                    result = this.girl - myKey.getGirl();
                }
            }
        }
        return result;
    }
}
