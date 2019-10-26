package gov.fda.nctr.dbmd;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonIgnore;


public class ForeignKey implements Serializable {

    private RelId sourceRelationId;

    private RelId targetRelationId;

    private List<Component> foreignKeyComponents;

    public enum EquationStyle {SOURCE_ON_LEFTHAND_SIDE, TARGET_ON_LEFTHAND_SIDE}

    public ForeignKey
        (
            RelId sourceRelationId,
            RelId targetRelationId,
            List<Component> foreignKeyComponents
        )
    {
        this.sourceRelationId = requireNonNull(sourceRelationId);
        this.targetRelationId = requireNonNull(targetRelationId);
        this.foreignKeyComponents = unmodifiableList(new ArrayList<>(requireNonNull(foreignKeyComponents)));
    }

    protected ForeignKey() {}

    public RelId getSourceRelationId() { return sourceRelationId; }

    public RelId getTargetRelationId() { return targetRelationId; }

    public List<Component> getForeignKeyComponents() { return foreignKeyComponents; }

    @JsonIgnore()
    public List<String> getSourceFieldNames()
    {
        List<String> names = new ArrayList<>();

        for(Component comp: foreignKeyComponents)
            names.add(comp.getForeignKeyFieldName());

        return names;
    }

    @JsonIgnore()
    public List<String> getTargetFieldNames()
    {
        List<String> names = new ArrayList<>();

        for(Component comp: foreignKeyComponents)
            names.add(comp.getPrimaryKeyFieldName());

        return names;
    }

    public String asEquation
        (
            String src_rel_alias,
            String tgt_rel_alias
        )
    {
        return asEquation(src_rel_alias, tgt_rel_alias, EquationStyle.SOURCE_ON_LEFTHAND_SIDE);
    }

    public String asEquation
        (
            String srcRelAlias,
            String tgtRelAlias,
            EquationStyle style
        )
    {
        StringBuilder sb = new StringBuilder();

        boolean srcFirst = style == EquationStyle.SOURCE_ON_LEFTHAND_SIDE;

        for ( Component fkc: foreignKeyComponents)
        {
            if ( sb.length() > 0 )
                sb.append(" and ");

            String fstAlias = srcFirst ? srcRelAlias :                tgtRelAlias;
            String fstFld =   srcFirst ? fkc.getForeignKeyFieldName() : fkc.getPrimaryKeyFieldName();

            String sndAlias = srcFirst ? tgtRelAlias :                srcRelAlias;
             String sndFld =   srcFirst ? fkc.getPrimaryKeyFieldName() : fkc.getForeignKeyFieldName();

            if ( fstAlias != null && fstAlias.length() > 0 )
            {
                sb.append(fstAlias);
                sb.append('.');
            }
            sb.append(fstFld);

            sb.append(" = ");

               if ( sndAlias != null && sndAlias.length() > 0 )
            {
                sb.append(sndAlias);
                sb.append('.');
            }
            sb.append(sndFld);
        }

        return sb.toString();
    }

    public boolean sourceFieldNamesSetEqualsNormalizedNamesSet(Set<String> normdReqdFkFieldNames)
    {
        if ( getForeignKeyComponents().size() != normdReqdFkFieldNames.size() )
            return false;

        Set<String> childFkFieldNames = new HashSet<>();

        for(ForeignKey.Component fk_comp: getForeignKeyComponents())
            childFkFieldNames.add(fk_comp.getForeignKeyFieldName());

        return childFkFieldNames.equals(normdReqdFkFieldNames);
    }

    public static class Component
    {
        private String foreignKeyFieldName;

        private String primaryKeyFieldName;

        public Component(String fkName, String pkName)
        {
            foreignKeyFieldName = fkName;
            primaryKeyFieldName = pkName;
        }

        protected Component() {}

        public String getForeignKeyFieldName() { return foreignKeyFieldName; }

        public String getPrimaryKeyFieldName() { return primaryKeyFieldName; }
    }
}

