package gov.fda.nctr.dbmd;

import java.util.*;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.empty;
import static java.util.stream.Collectors.toList;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;


@JsonPropertyOrder({"relationId", "relationType", "relationComment", "fields"})
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
        this.relId = requireNonNull(relId);
        this.relType = requireNonNull(relType);
        this.relComment = requireNonNull(relComment);
        this.fields = unmodifiableList(new ArrayList<>(requireNonNull(fields)));
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
            f.getPrimaryKeyPartNumber().ifPresent(n -> pks.add(f));
        }

        pks.sort(Comparator.comparingInt(f -> f.getPrimaryKeyPartNumber().orElse(0)));

        return pks;
    }

    @JsonIgnore()
    public List<String> getPrimaryKeyFieldNames()
    {
        return getPrimaryKeyFieldNames(empty());
    }

    public List<String> getPrimaryKeyFieldNames(Optional<String> alias)
    {
        return
            getPrimaryKeyFields().stream()
            .map(f -> alias.map(a -> a + "." + f.getName()).orElse(f.getName()))
            .collect(toList());
    }
}
