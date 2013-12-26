package doh.op.serde;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import doh.ds.MapKVDataSet;
import doh.op.Context;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.lang.reflect.Type;

public class MapKVDataSetJsonSerDe implements JsonSerializer<MapKVDataSet>, JsonDeserializer<MapKVDataSet> {

    private static final Configuration conf = new Configuration();
    @Override
    public MapKVDataSet deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
        Path path = new Path(json.getAsJsonPrimitive().getAsString());
        Context context1 = Context.create(conf, null);
        MapKVDataSet mkvds = new MapKVDataSet(path);
        mkvds.setContext(context1);
        return mkvds;
    }

    @Override
    public JsonElement serialize(MapKVDataSet src, Type typeOfSrc, JsonSerializationContext context) {
        return new JsonPrimitive(src.getPath().toString());
    }
}
