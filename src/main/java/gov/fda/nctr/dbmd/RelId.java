package gov.fda.nctr.dbmd;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Objects;
import java.util.Optional;
import static java.util.Objects.requireNonNull;


public final class RelId {

    private Optional<String> catalog;

    private Optional<String> schema;

    private String name;

    public RelId(Optional<String> catalog, Optional<String> schema, String name)
    {
        this.catalog = requireNonNull(catalog);
        this.schema = requireNonNull(schema);
        this.name = requireNonNull(name);
    }

    protected RelId() {}

    public Optional<String> getCatalog() { return catalog; }

    public Optional<String> getSchema() { return schema; }

    public String getName() { return name; }


    public String toString()
    {
        return getIdString();
    }

    @JsonIgnore
    public String getIdString()
    {
        return
            catalog.map(cat ->  "[" + cat + "]").orElse("") +
            schema.map(s -> s + ".").orElse("") +
            name;
    }


    public boolean equals(Object other)
    {
        if ( !(other instanceof RelId) )
            return false;
        else
        {
            RelId o = (RelId)other;
            return
                Objects.equals(catalog, o.catalog) &&
                Objects.equals(schema, o.schema) &&
                Objects.equals(name, o.name);
        }
    }

    public int hashCode()
    {
        return catalog.hashCode() + 3 * (schema.hashCode()  + 7 * name.hashCode());
    }
}
