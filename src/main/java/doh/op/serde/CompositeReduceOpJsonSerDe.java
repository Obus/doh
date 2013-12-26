package doh.op.serde;

import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import doh.op.OpSequence;
import doh.op.kvop.CompositeMapOp;
import doh.op.kvop.CompositeReduceOp;
import doh.op.kvop.KVUnoOp;
import doh.op.kvop.ReduceOp;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

public class CompositeReduceOpJsonSerDe implements JsonSerializer<CompositeReduceOp>, JsonDeserializer<CompositeReduceOp> {


    @Override
    public JsonElement serialize(CompositeReduceOp src, Type typeOfSrc, JsonSerializationContext context) {
        JsonArray result = new JsonArray();
        result.add(context.serialize(src.getReduceOp().getClass().getName()));
        result.add(context.serialize(src.getReduceOp()));
        result.add(context.serialize(src.getCompositeMapOp()));
        return result;
    }

    @Override
    public CompositeReduceOp deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
        JsonArray items = (JsonArray) json;
        try {
            Class reduceOpClass = Class.forName((String) context.deserialize(items.get(0), String.class));
            ReduceOp reduceOp = context.deserialize(items.get(1), reduceOpClass);
            CompositeMapOp compositeMapOp = context.deserialize(items.get(2), CompositeMapOp.class);
            return new CompositeReduceOp(reduceOp, compositeMapOp);
        } catch (ClassNotFoundException e) {
            throw new JsonParseException(e);
        }

    }


}
