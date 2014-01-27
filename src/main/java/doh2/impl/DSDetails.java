package doh2.impl;

import doh2.api.HDFSLocation;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Specific details about data set
 */
public class DSDetails {
    public Class<? extends OutputFormat> formatClass;
    public HDFSLocation location;
    public Integer numReducers;

    public Class<? extends InputFormat> inputFormatClass() {
        if (formatClass == null) {
            return null;
        }
        if (formatClass.equals(SequenceFileOutputFormat.class)) {
            return SequenceFileInputFormat.class;
        }
        if (formatClass.equals(TextOutputFormat.class)) {
            return TextInputFormat.class;
        }
        throw new IllegalArgumentException("Unknown output format type: " + formatClass.getName());
    }
}
