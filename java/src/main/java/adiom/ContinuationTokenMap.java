package adiom;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.protobuf.ByteString;

/**
 * Stores a mapping from namespace to continuation token, with binary serialization.
 */
public class ContinuationTokenMap {
    private final ConcurrentHashMap<String, String> tokenMap = new ConcurrentHashMap<>();

    public void put(String namespace, String token) {
        tokenMap.put(namespace, token);
    }

    public String get(String namespace) {
        return tokenMap.get(namespace);
    }

    public ConcurrentHashMap<String, String> getMap() {
        return tokenMap;
    }

    public ByteString serialize() {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(new HashMap<>(tokenMap)); // serialize as HashMap for compatibility
            oos.flush();
            return ByteString.copyFrom(bos.toByteArray());
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize ContinuationTokenMap", e);
        }
    }

    @SuppressWarnings("unchecked")
    public static ContinuationTokenMap deserialize(ByteString data) {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(data.toByteArray());
            ObjectInputStream ois = new ObjectInputStream(bis);
            Map<String, String> map = (Map<String, String>) ois.readObject();
            ContinuationTokenMap ctm = new ContinuationTokenMap();
            ctm.tokenMap.putAll(map);
            return ctm;
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize ContinuationTokenMap", e);
        }
    }
}
