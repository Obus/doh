package doh.op.serde;

import com.google.gson.*;
import doh.ds.MapKVDataSet;
import doh.op.OpSequence;
import doh.op.kvop.KVUnoOp;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class OpSequenceJsonSerDe implements JsonSerializer<OpSequence>, JsonDeserializer<OpSequence> {
    @Override
    public OpSequence deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
        JsonArray items = (JsonArray) new JsonParser().parse(json.getAsString());
        List<KVUnoOp> sequence = new ArrayList<KVUnoOp>();

        try {
            for(JsonElement e : items){
                JsonArray itemArr = (JsonArray) e;
                if (itemArr.size() != 2) {
                    throw new IllegalArgumentException();
                }
                String itemClassStr = context.deserialize(itemArr.get(0), String.class);
                sequence.add((KVUnoOp) context.deserialize(itemArr.get(1), Class.forName(itemClassStr)));
            }
        } catch (Exception e) {
            throw new JsonParseException(e);
        }

        return new OpSequence(sequence);
    }

    @Override
    public JsonElement serialize(OpSequence src, Type typeOfSrc, JsonSerializationContext context) {
        JsonArray result = new JsonArray();
        for(KVUnoOp item : src.getSequence()){
            JsonArray itemArr = new JsonArray();
            itemArr.add(context.serialize(item.getClass().getName()));
            itemArr.add(context.serialize(item));
            result.add(itemArr);
        }
        return new JsonPrimitive(result.toString());
    }
}
