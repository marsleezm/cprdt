package swift.crdt;

/**
 * System-wide unique identifier for CRDT object. Identification via table to
 * which the CRDT is associated and key under which the CRDT is stored.
 * 
 * @author annettebieniusa
 * 
 */
// TODO: provide custom serializer or Kryo-lize the class
public class CRDTIdentifier {
    private String table;
    private String key;

    public CRDTIdentifier() {
    }

    public CRDTIdentifier(String table, String key) {
        if (table == null || "".equals(table) | key == null | "".equals(key)) {
            throw new NullPointerException("CRDTIdentifier cannot have empty table or key");
        }
        this.table = table;
        this.key = key;
    }

    /**
     * @return table for an object to which the object is associated
     */
    public String getTable() {
        return this.table;
    }

    /**
     * @return key for an object under which the object is saved
     */
    public String getKey() {
        return this.key;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        return prime * table.hashCode() + key.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof CRDTIdentifier)) {
            return false;
        }
        CRDTIdentifier other = (CRDTIdentifier) obj;
        return table.equals(other.table) && key.equals(other.key);
    }

    public String toString() {
        return "CRDTId(" + table + "," + key + ")";
    }
}
