package gov.fda.nctr.dbmd;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;


@XmlRootElement(name="database-metadata", namespace="http://nctr.fda.gov/dbmd")
@XmlAccessorType(XmlAccessType.FIELD)
public class DBMD {

  @XmlAttribute(name="requested-owning-schema-name")
  String requestedOwningSchemaName;
  @XmlAttribute(name="case-sensitivity")
  CaseSensitivity caseSensitivity;
  
  @XmlElementWrapper(name = "relation-metadatas")
  @XmlElement(name="rel-md")
  List<RelMetaData> relMetaDatas;
  
  @XmlElementWrapper(name = "foreign-keys")
  @XmlElement(name="foreign-key")
  List<ForeignKey> foreignKeys;
  
  @XmlTransient
  Map<RelId,RelMetaData> relMDsByRelId;
  @XmlTransient
  Map<RelId,List<ForeignKey>> fksByParentRelId;
  @XmlTransient
  Map<RelId,List<ForeignKey>> fksByChildRelId;
  
  @XmlEnum
  public enum CaseSensitivity { INSENSITIVE_STORED_LOWER,
	  							INSENSITIVE_STORED_UPPER,
	  							INSENSITIVE_STORED_MIXED,
	  							SENSITIVE }
  
  public DBMD(String owningSchemaName,
              List<RelMetaData> relMetaDatas,
              List<ForeignKey> foreignKeys,
              CaseSensitivity caseSensitivity)
  {
	  super();
	  this.requestedOwningSchemaName = owningSchemaName;
	  this.relMetaDatas = Collections.unmodifiableList(new ArrayList<RelMetaData>(relMetaDatas));
	  this.foreignKeys = Collections.unmodifiableList(new ArrayList<ForeignKey>(foreignKeys));
	  this.caseSensitivity = caseSensitivity;
  }

  // No-args constructor for JAXB.
  protected DBMD() {}
  

  public String getRequestedOwningSchemaName()
  {
	  return requestedOwningSchemaName;
  }
  
  public CaseSensitivity getCaseSensitivity()
  {
	  return caseSensitivity;
  }

  public List<RelMetaData> getRelationMetaDatas()
  {
	  return relMetaDatas;
  }
  

  public List<ForeignKey> getForeignKeys()
  {
	  return foreignKeys;
  }

  
  public RelMetaData getRelationMetaData(RelId rel_id)
  {
	  return relMDsByRelId().get(rel_id);
  }
  
  public RelMetaData getRelationMetaData(String schema, String relname)
  {
	  return relMDsByRelId().get(toRelId(schema,relname));
  }

  
  public List<String> getFieldNames(RelId rel_id,
                                    String alias) // optional
  {
	  RelMetaData rel_md = getRelationMetaData(rel_id);
	  
	  if ( rel_md == null )
		  throw new IllegalArgumentException("Relation " + rel_id + " not found.");
	  
	  List<String> field_names = new ArrayList<String>();
	  
	  for(Field f: rel_md.getFields() )
		  field_names.add(alias != null ? alias + "." + f.getName() : f.getName());
	  
	  return field_names;
  }
  
  public List<String> getFieldNames(RelId rel_id)
  {
	  return getFieldNames(rel_id, null);
  }
  
  
  public List<String> getFieldNames(String schema, String relname)
  {
	  return getFieldNames(toRelId(schema,relname));
  }
  
  public List<String> getFieldNames(String schema, String relname, String alias)
  {
	  return getFieldNames(toRelId(schema,relname), alias);
  }
  
  
  public List<String> getPrimaryKeyFieldNames(RelId rel_id,
                                              String alias) // optional
  {
	  RelMetaData rel_md = getRelationMetaData(rel_id);
	  
	  if ( rel_md == null )
		  throw new IllegalArgumentException("Relation " + rel_id + " not found.");
	  
	  return rel_md.getPrimaryKeyFieldNames(alias);
  }

  public List<String> getPrimaryKeyFieldNames(RelId rel_id)
  {
	  return getPrimaryKeyFieldNames(rel_id, null);
  }


  public List<String> getPrimaryKeyFieldNames(String schema, String relname)
  {
	  return getPrimaryKeyFieldNames(toRelId(schema,relname));
  }

  public List<String> getPrimaryKeyFieldNames(String schema, String relname, String alias)
  {
	  return getPrimaryKeyFieldNames(toRelId(schema,relname), alias);
  }

  
  public List<ForeignKey> getForeignKeysToParentsFrom(RelId rel_id)
  {
	  return getForeignKeysFromTo(rel_id, null);
  }
  

  public List<ForeignKey> getForeignKeysToParentsFrom(String schema, String relname)
  {
	  return getForeignKeysFromTo(toRelId(schema, relname), null);
  }
  
  
  public List<ForeignKey> getForeignKeysFromChildrenTo(RelId rel_id)
  {
	  return getForeignKeysFromTo(null, rel_id);
  }
  
  public List<ForeignKey> getForeignKeysFromChildrenTo(String schema, String relname)
  {
	  return getForeignKeysFromTo(null, toRelId(schema,relname));
  }
 
  
  public List<ForeignKey> getForeignKeysFromTo(String from_schema,
                                               String from_relname,
                                               String to_schema,
                                               String to_relname)
  {
	  return getForeignKeysFromTo(toRelId(from_schema, from_relname),
	                              toRelId(to_schema, to_relname));
  }
  
  
  public List<ForeignKey> getForeignKeysFromTo(RelId child_rel_id,  // optional
                                               RelId parent_rel_id) // optional
  {
	  List<ForeignKey> res = new ArrayList<ForeignKey>();
	  
	  if ( child_rel_id == null && parent_rel_id == null )
		  res.addAll(foreignKeys);
	  else if ( child_rel_id != null && parent_rel_id != null )
	  {
		  res.addAll(fksByChildRelId(child_rel_id));
		  res.retainAll(fksByParentRelId(parent_rel_id));
	  }
	  else
		  res.addAll(child_rel_id != null ? fksByChildRelId(child_rel_id)
				                          : fksByParentRelId(parent_rel_id));
		  
	  return res;
  }
  

  /** Return the field names in the passed table involved in foreign keys (to parents). */
  public List<String> getForeignKeyFieldNames(RelId rel_id,
                                              String alias) // optional
  {
	  List<String> fk_fieldnames = new ArrayList<String>();

	  for(ForeignKey fk: getForeignKeysToParentsFrom(rel_id))
	  {
		  for(ForeignKey.Component fkcomp: fk.getForeignKeyComponents())
		  {
			  String name = alias == null ? fkcomp.getForeignKeyFieldName() : alias + "." + fkcomp.getForeignKeyFieldName();

			  if ( !fk_fieldnames.contains(name) )
				  fk_fieldnames.add(name);
		  }
	  }

	  return fk_fieldnames;
  }
  
  public Set<RelId> getMultiplyReferencingChildTablesForParent(RelId parent_rel_id)
  {
	  Set<RelId> rels = new HashSet<RelId>();
	  Set<RelId> repeated_rels = new HashSet<RelId>();
	  
	  for(ForeignKey fk: getForeignKeysFromChildrenTo(parent_rel_id))
	  {
		  if ( !rels.add(fk.getSourceRelationId()) )
			  repeated_rels.add(fk.getSourceRelationId());
	  }
	  
	  return repeated_rels;
  }
  
  public Set<RelId> getMultiplyReferencedParentTablesForChild(RelId child_rel_id)
  {
	  Set<RelId> rels = new HashSet<RelId>();
	  Set<RelId> repeated_rels = new HashSet<RelId>();
	  
	  for(ForeignKey fk: getForeignKeysToParentsFrom(child_rel_id))
	  {
		  if ( !rels.add(fk.getSourceRelationId()) )
			  repeated_rels.add(fk.getSourceRelationId());
	  }
	  
	  return repeated_rels;
  }
  
  
  public String normalizeDatabaseId(String id)
  {
      if (id == null || id.equals(""))
          return null;
      else if ( id.startsWith("\"") && id.endsWith("\"") )
      	return id; // TODO: maybe should strip the quotes from the value, needs testing
      else if ( caseSensitivity == CaseSensitivity.INSENSITIVE_STORED_LOWER )
          return id.toLowerCase();
      else if ( caseSensitivity == CaseSensitivity.INSENSITIVE_STORED_UPPER )
          return id.toUpperCase();
      else
          return id;
  }
  
  public Set<String> normalizeNames(Set<String> names)
  {
	  if ( names == null )
		  return null;
	  else
	  {
		  final Set<String> normd_names = new HashSet<String>();

		  for(String name: names)
			  normd_names.add(normalizeDatabaseId(name));

		  return normd_names;
	  }
  }
  
  
  public ForeignKey getForeignKeyHavingFieldSetAmong(Set<String> src_field_names, java.util.Collection<ForeignKey> fks)
  {
	  Set<String> normd_field_names = normalizeNames(src_field_names);

	  for(ForeignKey fk: fks)
	  {
		  if ( fk.sourceFieldNamesSetEqualsNormalizedNamesSet(normd_field_names) )
			  return fk;
	  }

	  return null;
  }

  
  public RelId toRelId(String catalog, String schema, String relname)
  {
	  return new RelId(normalizeDatabaseId(catalog),
	                   normalizeDatabaseId(schema),
	                   normalizeDatabaseId(relname));
  }
  
  public RelId toRelId(String schema, String relname)
  {
	  return new RelId(null,
	                   normalizeDatabaseId(schema),
	                   normalizeDatabaseId(relname));
  }
  
  public RelId toRelId(String possibly_schema_qualified_relname)
  {
	String schema;
	String relname;
	
  	int dotix = possibly_schema_qualified_relname.indexOf('.');
  	
	if ( dotix == -1 )
	{
		schema = getRequestedOwningSchemaName() != null ? getRequestedOwningSchemaName() : null;
		relname = possibly_schema_qualified_relname;
	}
	else
	{
		schema = possibly_schema_qualified_relname.substring(0, dotix);
		relname = possibly_schema_qualified_relname.substring(dotix + 1);
	}
	
	return new RelId(null,
	                 normalizeDatabaseId(schema),
	                 normalizeDatabaseId(relname));
  }
  
  
  
  /////////////////////////////////////////////////////////
  // Derived data / caching methods
  
  protected Map<RelId, RelMetaData> relMDsByRelId()
  {
	  if ( relMDsByRelId == null )
		  initDerivedData();
	  
	  return relMDsByRelId;
  }
  
  private List<ForeignKey> fksByParentRelId(RelId rel_id)
  {
	  if ( fksByParentRelId == null )
		  initDerivedData();
	  
	  List<ForeignKey> fks = fksByParentRelId.get(rel_id);
	  if (fks != null)
		  return fks;
	  else
		  return Collections.emptyList();
  }
  
  private List<ForeignKey> fksByChildRelId(RelId rel_id)
  {
	  if ( fksByChildRelId == null )
		  initDerivedData();
	  
	  List<ForeignKey> fks = fksByChildRelId.get(rel_id);
	  if (fks != null)
		  return fks;
	  else
		  return Collections.emptyList();
  }
  
  protected void initDerivedData() 
  {
	  relMDsByRelId = new HashMap<RelId,RelMetaData>();
	  if ( relMetaDatas != null )
	  {
		  for(RelMetaData rel_md: relMetaDatas)
			  relMDsByRelId.put(rel_md.getRelationId(), rel_md);
	  }
	  
	  fksByParentRelId = new HashMap<RelId,List<ForeignKey>>();
	  fksByChildRelId = new HashMap<RelId,List<ForeignKey>>();
	  if ( foreignKeys != null )
	  {
		  for(ForeignKey fk: foreignKeys)
		  {
			  RelId src_relid = fk.getSourceRelationId();
			  RelId tgt_relid = fk.getTargetRelationId();

			  List<ForeignKey> fks_from_child = fksByChildRelId.get(src_relid);
			  if ( fks_from_child == null )
				  fksByChildRelId.put(src_relid, fks_from_child = new ArrayList<ForeignKey>());
			  fks_from_child.add(fk);

			  List<ForeignKey> fks_to_parent = fksByParentRelId.get(tgt_relid);
			  if ( fks_to_parent == null )
				  fksByParentRelId.put(tgt_relid, fks_to_parent = new ArrayList<ForeignKey>());
			  fks_to_parent.add(fk);
		  }
	  }
  }
  
  // Derived data / caching methods
  /////////////////////////////////////////////////////////

  
  public void writeXML(OutputStream os) throws JAXBException, IOException
  {
	  JAXBContext context = JAXBContext.newInstance(getClass().getPackage().getName(), DBMD.class.getClassLoader());
	  Marshaller m = context.createMarshaller();
	  m.setProperty( Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE );
	  m.marshal(this, os);
	  os.flush();
  }

  public static DBMD readXML(InputStream is, boolean close_stream) throws JAXBException, IOException
  {
	  String packageName = DBMD.class.getPackage().getName();
	  
	  Unmarshaller u = JAXBContext.newInstance(packageName, DBMD.class.getClassLoader()).createUnmarshaller();

	  DBMD dbmd = (DBMD)u.unmarshal(is);
	  
	  if ( close_stream )
		  is.close();
	  
	  return dbmd;
  }
  
  public static DBMD readXML(InputStream is) throws JAXBException, IOException
  {
	  return readXML(is, false);
  }
}

