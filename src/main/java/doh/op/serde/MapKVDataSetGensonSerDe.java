package doh.op.serde;

import com.owlike.genson.Context;
import com.owlike.genson.Converter;
import com.owlike.genson.TransformationException;
import com.owlike.genson.stream.ObjectReader;
import com.owlike.genson.stream.ObjectWriter;
import doh.api.ds.HDFSLocation;
import doh.ds.MapKVDataSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class MapKVDataSetGensonSerDe implements Converter<MapKVDataSet> {

    private static final Configuration conf = new Configuration();

    @Override
    public void serialize(MapKVDataSet object, ObjectWriter writer, Context ctx) throws TransformationException, IOException {
        Path path = ((HDFSLocation.SingleHDFSLocation) object.getLocation()).getPath();
        writer.beginObject().writeName("path").writeValue(path.toString()).endObject();
    }

    @Override
    public MapKVDataSet deserialize(ObjectReader reader, Context ctx) throws TransformationException, IOException {
        String pathStr = reader.beginObject().valueAsString();
        reader.endObject();
        Path path = new Path(pathStr);
        doh.op.Context context1 = doh.op.Context.create(conf);
        MapKVDataSet mkvds = new MapKVDataSet(new HDFSLocation.SingleHDFSLocation(path));
        mkvds.setContext(context1);
        return mkvds;

    }
}
