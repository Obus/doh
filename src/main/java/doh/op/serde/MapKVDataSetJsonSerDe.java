package doh.op.serde;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import doh.api.Context;
import doh2.api.HDFSLocation;
import doh.ds.MapKVDS;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.lang.reflect.Type;

public class MapKVDataSetJsonSerDe implements JsonSerializer<MapKVDS>, JsonDeserializer<MapKVDS> {

    private static final Configuration conf = new Configuration();

    @Override
    public MapKVDS deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
        Path path = new Path(json.getAsJsonPrimitive().getAsString());
        Context context1 = Context.create(conf);
        MapKVDS mkvds = new MapKVDS(new HDFSLocation.SingleHDFSLocation(path));
        mkvds.setContext(context1);
        return mkvds;
    }

    @Override
    public JsonElement serialize(MapKVDS src, Type typeOfSrc, JsonSerializationContext context) {
        Path path = ((HDFSLocation.SingleHDFSLocation) src.getLocation()).getPath();
        return new JsonPrimitive(path.toString());
    }
}
