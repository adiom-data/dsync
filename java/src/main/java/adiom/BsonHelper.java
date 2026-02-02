package adiom;

import java.nio.ByteBuffer;
import java.util.List;

import org.bson.BsonType;
import org.bson.ByteBufNIO;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.ByteBufferBsonInput;

import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.PartitionKeyBuilder;
import com.google.protobuf.ByteString;

public class BsonHelper {

    public static List<String> getIdParts(List<adiom.v1.Messages.BsonValue> bvs) {
        if (bvs.isEmpty()) {
            throw new IllegalArgumentException("Must not have empty ids.");
        }
        List<String> res = new java.util.ArrayList<>(bvs.size());
        for (adiom.v1.Messages.BsonValue bv : bvs) {
            BsonType typ = BsonType.findByValue(bv.getType());
            ByteBufferBsonInput input = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(bv.getData().toByteArray())));
            if (typ == BsonType.STRING) {
                String s = input.readString();
                input.close();
                res.add(s);
            } else {
                input.close();
                throw new IllegalArgumentException("Only String id currently supported.");
            }
        }
        return res;
    }

    public static String getId(List<adiom.v1.Messages.BsonValue> bvs) {
        if (bvs.isEmpty()) {
            throw new IllegalArgumentException("Must not have empty ids.");
        }
        adiom.v1.Messages.BsonValue bv = bvs.get(bvs.size() - 1);
        BsonType typ = BsonType.findByValue(bv.getType());
        ByteBufferBsonInput input = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(bv.getData().toByteArray())));
        if (typ == BsonType.STRING) {
            String s = input.readString();
            input.close();
            return s;
        } else {
            input.close();
            throw new IllegalArgumentException("Only String id currently supported.");
        }
    }

    public static adiom.v1.Messages.BsonValue toId(String key, String id) {
        BasicOutputBuffer outputBuffer = new BasicOutputBuffer(id.length() + 5);
        outputBuffer.writeString(id);
        ByteString bs = ByteString.copyFrom(outputBuffer.getInternalBuffer(), 0, outputBuffer.getSize());
        outputBuffer.close();
        return adiom.v1.Messages.BsonValue.newBuilder().setName(key).setData(bs).setType(BsonType.STRING.getValue()).build();
    }

    public static PartitionKey getPartitionKey(List<adiom.v1.Messages.BsonValue> bvs) {
        if (bvs.isEmpty()) {
            throw new IllegalArgumentException("Must not have empty ids.");
        }
        if (bvs.size() == 1) {
            return new PartitionKeyBuilder().add(BsonHelper.getId(bvs)).build();
        }
        PartitionKeyBuilder pkb = new PartitionKeyBuilder();
        for (int i = 0; i < bvs.size() - 1; ++i) {
            adiom.v1.Messages.BsonValue bv = bvs.get(i);
            BsonType typ = BsonType.findByValue(bv.getType());
            ByteBufferBsonInput input = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(bv.getData().toByteArray())));
            if (typ == BsonType.STRING) {
                String s = input.readString();
                input.close();
                pkb.add(s);
            } else {
                input.close();
                throw new IllegalArgumentException("Only String id currently supported.");
            }
        }
        return pkb.build();
    } 
}
