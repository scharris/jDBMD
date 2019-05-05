package gov.fda.nctr.dbmd;

import java.util.Optional;

import gov.fda.nctr.dbmd.RelMetaData.RelType;


public class RelDescr {

    private RelId relId;

    private RelType relType;

    private Optional<String> relComment;

    public RelDescr(RelId relId, RelType relType, Optional<String> relComment)
    {
        super();
        this.relId = relId;
        this.relType = relType;
        this.relComment = relComment;
    }

    protected RelDescr() {}

    public RelId getRelationId() { return relId; }

    public RelType getRelationType() { return relType; }

    public Optional<String> getRelationComment() { return relComment; }

}