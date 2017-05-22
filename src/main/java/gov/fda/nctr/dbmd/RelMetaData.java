package gov.fda.nctr.dbmd;

import java.io.Serializable;
import java.util.*;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlEnum;


@XmlAccessorType(XmlAccessType.FIELD)
public class RelMetaData implements Serializable {

    @XmlElement(name="rel-id")
    RelId relId;

    @XmlAttribute(name="rel-type")
    RelType relType;

    @XmlElement(name="rel-comment")
    String relComment;

    @XmlElementWrapper(name = "fields")
    @XmlElement(name="field")
    List<Field> fields;

    @XmlEnum
    public enum RelType { Table, View, Unknown }


    public RelMetaData(RelId rel_id,
                       RelType rel_type,
                       String rel_comment,
                       List<Field> fs)
    {
        relId = rel_id;
        relType = rel_type;
        relComment = rel_comment;
        fields = fs;
    }

    protected RelMetaData() {}

    public RelId getRelationId() { return relId; }

    public RelType getRelationType() { return relType; }

    public String getRelationComment() { return relComment; }

    public List<Field> getFields() { return fields; }


    public List<Field> getPrimaryKeyFields()
    {
        List<Field> pks = new ArrayList<>();

        for(Field f: fields)
        {
            if ( f.pkPartNum != null )
                pks.add(f);
        }

        pks.sort(Comparator.comparingInt(f -> f.pkPartNum));

        return pks;
    }

    public List<String> getPrimaryKeyFieldNames()
    {
        return getPrimaryKeyFieldNames(null);
    }

    public List<String> getPrimaryKeyFieldNames(String alias)
    {
        List<String> names = new ArrayList<>();

        for(Field f: getPrimaryKeyFields())
            names.add(alias != null ? alias + "." + f.getName() : f.getName());

        return names;
    }


    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((fields == null) ? 0 : fields.hashCode());
        result = prime * result + ((relComment == null) ? 0 : relComment.hashCode());
        result = prime * result + ((relId == null) ? 0 : relId.hashCode());
        result = prime * result + ((relType == null) ? 0 : relType.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        RelMetaData other = (RelMetaData) obj;
        if (fields == null)
        {
            if (other.fields != null)
                return false;
        }
        else if (!fields.equals(other.fields))
            return false;
        if (relComment == null)
        {
            if (other.relComment != null)
                return false;
        }
        else if (!relComment.equals(other.relComment))
            return false;
        if (relId == null)
        {
            if (other.relId != null)
                return false;
        }
        else if (!relId.equals(other.relId))
            return false;
        return relType == other.relType;
    }

    private static final long serialVersionUID = 1L;
}
