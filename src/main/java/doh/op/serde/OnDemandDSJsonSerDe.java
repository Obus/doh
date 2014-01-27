package doh.op.serde;


import com.google.gson.*;
import doh.api.Context;
import doh2.api.DSContext;
import doh2.api.HDFSLocation;
import doh2.impl.OnDemandDS;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.lang.reflect.Type;

public class OnDemandDSJsonSerDe implements JsonSerializer<OnDemandDS>, JsonDeserializer<OnDemandDS> {

    private static final Configuration conf = new Configuration();

    @Override
    public OnDemandDS deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
        Path path = new Path(json.getAsJsonPrimitive().getAsString());
        DSContext dsContext = new DSContext(conf, null, null);
        OnDemandDS mkvds = new OnDemandDS(dsContext, new HDFSLocation.SingleHDFSLocation(path));
        return mkvds;
    }

    @Override
    public JsonElement serialize(OnDemandDS src, Type typeOfSrc, JsonSerializationContext context) {
        Path path = ((HDFSLocation.SingleHDFSLocation) src.getLocation()).getPath();
        return new JsonPrimitive(path.toString());
    }
}
