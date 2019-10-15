package gov.fda.nctr.dbmd;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Objects;
import java.util.Optional;
import static java.util.Objects.requireNonNull;


public final class RelId {

    private Optional<String> schema;

    private String name;

    public RelId(Optional<String> schema, String name)
    {
        this.schema = requireNonNull(schema);
        this.name = requireNonNull(name);
    }

    protected RelId() {}

    public Optional<String> getSchema() { return schema; }

    public String getName() { return name; }


    public String toString()
    {
        return getIdString();
    }

    @JsonIgnore
    public String getIdString()
    {
        return schema.map(s -> s + ".").orElse("") + name;
    }


    public boolean equals(Object other)
    {
        if ( !(other instanceof RelId) )
            return false;
        else
        {
            RelId o = (RelId)other;
            return
                Objects.equals(schema, o.schema) &&
                Objects.equals(name, o.name);
        }
    }

    public int hashCode()
    {
        return (schema.hashCode()  + 7 * name.hashCode());
    }
}
