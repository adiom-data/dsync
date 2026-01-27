package adiom;

import java.util.List;
import java.util.Map;

public class Config {
    public Map<String, NamespaceConfig> namespaces;
    public List<String> cosmosInternalKeys;
}

class NamespaceConfig {
    public int pagesize;
    public int partitionfanout;
    public int prefetch;
}