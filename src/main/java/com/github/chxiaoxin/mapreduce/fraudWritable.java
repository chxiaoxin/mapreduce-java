package com.github.chxiaoxin.mapreduce;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class fraudWritable implements Writable {

    private String returnDate;
    private String receivingDate;
    private boolean isReturnOrder;

    public fraudWritable(){}

    public fraudWritable(String returnDate, String receivingDate, boolean isReturnOrder) {
        this.returnDate = returnDate;
        this.receivingDate = receivingDate;
        this.isReturnOrder = isReturnOrder;
    }

    public String getReturnDate() {
        return this.returnDate;
    }

    public String getReceivingDate() {
        return this.receivingDate;
    }

    public Boolean getIsReturnOrder() {
        return this.isReturnOrder;
    }

    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeString(dataOutput, this.returnDate);
        WritableUtils.writeString(dataOutput, this.receivingDate);
        dataOutput.writeBoolean(this.isReturnOrder);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.returnDate = WritableUtils.readString(dataInput);
        this.receivingDate = WritableUtils.readString(dataInput);
        this.isReturnOrder = dataInput.readBoolean();
    }
}