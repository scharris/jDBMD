package gov.fda.nctr.dbmd;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;

import gov.fda.nctr.dbmd.RelMetaData.RelType;


@XmlAccessorType(XmlAccessType.FIELD)
public class RelDescr implements Serializable {

    @XmlElement(name="rel-id")
    private RelId relId;

    @XmlAttribute(name="rel-type")
    private RelType relType;

    @XmlElement(name="rel-comment")
    private String relComment;

    public RelDescr(RelId relId, RelType relType, String relComment)
    {
        super();
        this.relId = relId;
        this.relType = relType;
        this.relComment = relComment;
    }

    protected RelDescr() {}

    public RelId getRelationId()
    {
        return relId;
    }

    public RelType getRelationType()
    {
        return relType;
    }

    public String getRelationComment()
    {
        return relComment;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
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
        RelDescr other = (RelDescr) obj;
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