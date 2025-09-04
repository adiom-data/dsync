package adiom;

import java.util.Map;

public class Config {
    public Map<String, NamespaceConfig> namespaces;
}

class NamespaceConfig {
    public int pagesize;
    public int partitionfanout;
}