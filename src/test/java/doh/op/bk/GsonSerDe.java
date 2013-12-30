package doh.op.bk;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.synqera.bigkore.rank.shoplist.RankedFilter;
import doh.ds.MapKVDataSet;

import java.lang.reflect.Type;

public class GsonSerDe {
    public static class RankedFilterGsonSerDe implements JsonSerializer<RankedFilter>, JsonDeserializer<RankedFilter> {
        @Override
        public RankedFilter deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
            JsonObject jsonObject = json.getAsJsonObject();
            String type = jsonObject.get("type").getAsString();
            JsonElement element = jsonObject.get("properties");

            try {
                return context.deserialize(element, Class.forName(type));
            } catch (ClassNotFoundException cnfe) {
                throw new JsonParseException("Unknown element type: " + type, cnfe);
            }
        }

        @Override
        public JsonElement serialize(RankedFilter src, Type typeOfSrc, JsonSerializationContext context) {
            JsonObject jsonObject = new JsonObject();
            jsonObject.add("type", new JsonPrimitive(src.getClass().getName()));
            jsonObject.add("properties", context.serialize(src, src.getClass()));
            return jsonObject;
        }
    }
}
