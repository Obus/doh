package doh2.impl;

import doh2.api.HDFSLocation;
import org.apache.hadoop.mapreduce.OutputFormat;

public class DSDetails {
    public Class<? extends OutputFormat> formatClass;
    public HDFSLocation location;
    public Integer numReducers;
}
