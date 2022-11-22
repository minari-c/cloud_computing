package com.B202044051.SecondarySort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class DateKey implements WritableComparable<DateKey> {
    private String year;
    private Integer month;

    public DateKey() {

    }

    public DateKey(String year, Integer date) {
        this.year = year;
        this.month = date;
    }

    public String getYear() {
        return this.year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public Integer getMonth() {
        return this.month;
    }

    public void setMonth(Integer month) {
        this.month = month;
    }

    @Override
    public String toString() {
        return String.format("%s,%s", this.year, this.month.toString());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.year = WritableUtils.readString(in);
        this.month = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, this.year);
        out.writeInt(this.month);
    }

    @Override
    public int compareTo(DateKey key) {
        int is_cmp_year = this.year.compareTo(key.year);
        int is_cmp_month = this.month.compareTo(key.month);

        return is_cmp_year & is_cmp_month;
    }
}

