package gov.fda.nctr.dbmd;

import java.util.*;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlEnum;


@XmlAccessorType(XmlAccessType.FIELD)
public class RelMetaData {
    
	@XmlEnum
    public enum RelType { Table, View, Unknown }
    
	@XmlElement(name="rel-id")
    RelId relId;
    
    @XmlAttribute(name="rel-type")
    RelType relType;
    
    @XmlElement(name="rel-comment")
    String relComment;
    
    @XmlElementWrapper(name = "fields")
    @XmlElement(name="field")
    List<Field> fields;
    

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
    	List<Field> pks = new ArrayList<Field>();
    	
    	for(Field f: fields)
    	{
    		if ( f.pkPartNum != null )
    			pks.add(f);
    	}
    	
    	Collections.sort(pks, new Comparator<Field>(){
    		public int compare(Field f1, Field f2)
    		{
    			return f1.pkPartNum - f2.pkPartNum;
    		}
    	});
    	
    	return pks;
    }
    
    public List<String> getPrimaryKeyFieldNames()
    {
    	return getPrimaryKeyFieldNames(null);
    }
    
    public List<String> getPrimaryKeyFieldNames(String alias)
    {
    	List<String> names = new ArrayList<String>();
    	
    	for(Field f: getPrimaryKeyFields())
    		names.add(alias != null ? alias + "." + f.getName() : f.getName());
    	
    	return names;
    }
    
}
