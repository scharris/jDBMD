package gov.fda.nctr.dbmd;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;


@XmlAccessorType(XmlAccessType.FIELD)
public class ForeignKey {

	@XmlElement(name="src-rel")
    RelId srcRel;
    
	@XmlElement(name="tgt-rel")
    RelId tgtRel;
	
	@XmlElement(name="component")
    List<Component> components;
	
	
    public enum EquationStyle {SOURCE_ON_LEFTHAND_SIDE, TARGET_ON_LEFTHAND_SIDE} 
    
    
    public ForeignKey(RelId src, RelId tgt, List<Component> fk_comps)
    {
        srcRel = src;
        tgtRel = tgt;
        components = fk_comps;
    }
    
    protected ForeignKey() {}

    public RelId getSourceRelationId()
    {
        return srcRel;
    }

    public RelId getTargetRelationId()
    {
        return tgtRel;
    }

    public List<Component> getForeignKeyComponents()
    {
        return components;
    }
    
    public List<String> getSourceFieldNames()
    {
    	List<String> names = new ArrayList<String>();
    	
    	for(Component comp: components)
    		names.add(comp.getForeignKeyFieldName());
    	
    	return names;
    }
    
    public List<String> getTargetFieldNames()
    {
    	List<String> names = new ArrayList<String>();
    	
    	for(Component comp: components)
    		names.add(comp.getPrimaryKeyFieldName());
    	
    	return names;
    }

    
    public String asEquation(String src_rel_alias, String tgt_rel_alias)
    {
    	return asEquation(src_rel_alias, tgt_rel_alias, EquationStyle.SOURCE_ON_LEFTHAND_SIDE);
    }
    
    
    public String asEquation(String src_rel_alias, 
                             String tgt_rel_alias, 
                             EquationStyle style)
    {
    	StringBuilder sb = new StringBuilder();

    	boolean src_first = style == EquationStyle.SOURCE_ON_LEFTHAND_SIDE;
    	
    	for(Component fkc: components)
    	{
    		if ( sb.length() > 0 )
    			sb.append(" and ");
    		
    		String fst_alias = src_first ? src_rel_alias :                tgt_rel_alias;
    		String fst_fld =   src_first ? fkc.getForeignKeyFieldName() : fkc.getPrimaryKeyFieldName();
    		
    		String snd_alias = src_first ? tgt_rel_alias :                src_rel_alias;
     		String snd_fld =   src_first ? fkc.getPrimaryKeyFieldName() : fkc.getForeignKeyFieldName();
    		
    		if ( fst_alias != null && fst_alias.length() > 0 )
    		{
    			sb.append(fst_alias);
    			sb.append('.');
    		}
    		sb.append(fst_fld);
    		
    		sb.append(" = ");
    		
       		if ( snd_alias != null && snd_alias.length() > 0 )
    		{
    			sb.append(snd_alias);
    			sb.append('.');
    		}
    		sb.append(snd_fld);
    	}
    	
    	return sb.toString();
    }
    
	public boolean sourceFieldNamesSetEqualsNormalizedNamesSet(Set<String> normd_reqd_fk_field_names)
	{
		if ( getForeignKeyComponents().size() != normd_reqd_fk_field_names.size() )
			return false;
		
		Set<String> child_fk_field_names = new HashSet<String>();
		
		for(ForeignKey.Component fk_comp: getForeignKeyComponents())
			child_fk_field_names.add(fk_comp.getForeignKeyFieldName());

		return child_fk_field_names.equals(normd_reqd_fk_field_names);
	}

    
    @XmlAccessorType(XmlAccessType.FIELD)
    public static class Component {
    	
        
    	@XmlAttribute(name="fk-field")
        String fkFieldName;
    	
    	@XmlAttribute(name="pk-field")
        String pkFieldName;
    	
    	
        public Component(String fk_name, String pk_name)
        {
            fkFieldName = fk_name;
            pkFieldName = pk_name;
        }
        
        protected Component() {}

        
        public String getForeignKeyFieldName()
        {
            return fkFieldName;
        }

        public String getPrimaryKeyFieldName()
        {
            return pkFieldName;
        }
    }
}

