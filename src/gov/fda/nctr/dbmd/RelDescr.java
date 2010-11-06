package gov.fda.nctr.dbmd;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;

import gov.fda.nctr.dbmd.RelMetaData.RelType;


@XmlAccessorType(XmlAccessType.FIELD)
public class RelDescr {

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
}