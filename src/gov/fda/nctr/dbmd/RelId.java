package gov.fda.nctr.dbmd;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;


@XmlAccessorType(XmlAccessType.FIELD)
public class RelId {
    
    @XmlAttribute
    String catalog;
    
    @XmlAttribute
    String schema;
    
    @XmlAttribute
    String name;
    
    public RelId(String catalog, String schema, String name)
    {
        this.catalog = catalog;
        this.schema = schema;
        this.name = name;
    }
    
    protected RelId() {}
    
    public String getIdString()
    {
        return ((catalog != null ? "[" + catalog + "]" : "") +
               (schema != null ? schema + "." : "") +
               name).toLowerCase();
    }
    
    public String toString()
    {
    	return getIdString();
    }
    
    public String getCatalog()
    {
        return catalog;
    }
    
    public String getSchema()
    {
        return schema;
    }
    
    public String getName()
    {
        return name;
    }
    
    public boolean equals(Object other)
    {
        if ( !(other instanceof RelId) )
            return false;
        else
        {
            RelId o = (RelId)other;
            return eq(catalog, o.catalog) &&
                   eq(schema, o.schema) &&
                   eq(name, o.name);
        }
    }
    
    public int hashCode()
    {
        return (catalog != null ? catalog.hashCode() : 0) +
               (schema != null ? schema.hashCode() : 0) +
               name.hashCode();
    }
    
    static boolean eq(Object o1, Object o2)
    {
        return (o1 == null && o2 == null) || o1.equals(o2);
    }
}
