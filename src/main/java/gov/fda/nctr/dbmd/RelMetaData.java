package gov.fda.nctr.dbmd;

import java.util.*;

import com.fasterxml.jackson.annotation.JsonIgnore;

import static java.util.stream.Collectors.toList;


public class RelMetaData {

    private RelId relId;

    private RelType relType;

    private Optional<String> relComment;

    private List<Field> fields;

    public enum RelType { Table, View, Unknown }


    public RelMetaData
        (
            RelId relId,
            RelType relType,
            Optional<String> relComment,
            List<Field> fields
        )
    {
        this.relId = relId;
        this.relType = relType;
        this.relComment = relComment;
        this.fields = fields;
    }

    protected RelMetaData() {}

    public RelId getRelationId() { return relId; }

    public RelType getRelationType() { return relType; }

    public Optional<String> getRelationComment() { return relComment; }

    public List<Field> getFields() { return fields; }

    @JsonIgnore()
    public List<Field> getPrimaryKeyFields()
    {
        List<Field> pks = new ArrayList<>();

        for ( Field f: fields )
        {
            if ( f.getPrimaryKeyPartNumber() != null )
                pks.add(f);
        }

        pks.sort(Comparator.comparingInt(f -> f.getPrimaryKeyPartNumber()));

        return pks;
    }

    @JsonIgnore()
    public List<String> getPrimaryKeyFieldNames()
    {
        return getPrimaryKeyFieldNames(null);
    }

    public List<String> getPrimaryKeyFieldNames(Optional<String> alias)
    {
        return
            getPrimaryKeyFields().stream()
            .map(f -> alias.map(a -> a + "." + f.getName()).orElse(f.getName()))
            .collect(toList());
    }
}
