package tn.enit.tp1;


import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
public class NumPair implements Writable {
    private int sum;
    private int count;

    public NumPair() {
    }

    public NumPair(int sum, int count) {
        this.sum = sum;
        this.count = count;
    }

    public int getSum() {
        return sum;
    }

    public int getCount() {
        return count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(sum);
        out.writeInt(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        sum = in.readInt();
        count = in.readInt();
    }

    @Override
    public String toString() {
        return sum + "\t" + count;
    }
}