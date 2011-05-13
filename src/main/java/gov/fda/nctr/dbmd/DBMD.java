package gov.fda.nctr.dbmd;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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

  @XmlAttribute(name="dbms-name")
  String dbmsName;

  @XmlAttribute(name="dbms-version")
  String dbmsVersion;

  @XmlAttribute(name="dbms-major-version")
  int dbmsMajorVersion;

  @XmlAttribute(name="dbms-minor-version")
  int dbmsMinorVersion;


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

  public enum ForeignKeyScope { REGISTERED_TABLES_ONLY, ALL_FKS }

  public DBMD(String owningSchemaName,
              List<RelMetaData> relMetaDatas,
              List<ForeignKey> foreignKeys,
              CaseSensitivity caseSensitivity,
              String dbms_name,
              String dbms_ver_str,
              int dbms_major_ver,
              int dbms_minor_ver)
  {
      super();
      this.requestedOwningSchemaName = owningSchemaName;
      this.relMetaDatas = sortedMds(relMetaDatas);
      this.foreignKeys = sortedFks(foreignKeys);
      this.caseSensitivity = caseSensitivity;
      this.dbmsName = dbms_name;
      this.dbmsVersion = dbms_ver_str;
      this.dbmsMajorVersion = dbms_major_ver;
      this.dbmsMinorVersion = dbms_minor_ver;
  }


  // No-args constructor for JAXB.

  DBMD() {}


  public String getRequestedOwningSchemaName()
  {
      return requestedOwningSchemaName;
  }

  public CaseSensitivity getCaseSensitivity()
  {
      return caseSensitivity;
  }

  public String getDbmsName()
  {
      return dbmsName;
  }

  public String getDbmsVersion()
  {
      return dbmsVersion;
  }

  public int getDbmsMajorVersion()
  {
      return dbmsMajorVersion;
  }

  public int getDbmsMinorVersion()
  {
      return dbmsMinorVersion;
  }


  public List<RelMetaData> getRelationMetaDatas()
  {
      return relMetaDatas;
  }


  public List<RelId> getRelationIds()
  {
      List<RelId> relids = new ArrayList<RelId>();

      for(RelMetaData relmd: relMetaDatas)
          relids.add(relmd.getRelationId());

      return relids;
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
                                               RelId parent_rel_id, // optional
                                               ForeignKeyScope fks_incl)
  {
      List<ForeignKey> res = new ArrayList<ForeignKey>();

      if ( child_rel_id == null && parent_rel_id == null )
      {
          res.addAll(foreignKeys);
      }
      else if ( child_rel_id != null && parent_rel_id != null )
      {
          res.addAll(fksByChildRelId(child_rel_id));
          res.retainAll(fksByParentRelId(parent_rel_id));
      }
      else
          res.addAll(child_rel_id != null ? fksByChildRelId(child_rel_id)
                                          : fksByParentRelId(parent_rel_id));

      if ( fks_incl == ForeignKeyScope.REGISTERED_TABLES_ONLY )
      {
          List<ForeignKey> res_filtered = new ArrayList<ForeignKey>();

          for(ForeignKey fk: res)
              if ( getRelationMetaData(fk.getSourceRelationId()) != null &&
                   getRelationMetaData(fk.getTargetRelationId()) != null )
                  res_filtered.add(fk);

          return res_filtered;
      }
      else
          return res;
  }

  public List<ForeignKey> getForeignKeysFromTo(RelId child_rel_id,  // optional
                                               RelId parent_rel_id) // optional
  {
      return getForeignKeysFromTo(child_rel_id,
                                  parent_rel_id,
                                  ForeignKeyScope.REGISTERED_TABLES_ONLY);
  }

  /** Return a single foreign key between the passed tables, having the specified field names if specified.
   *  Returns null if no such foreign key is found, or throws IllegalArgumentException if multiple foreign keys satisfy the requirements.
   */
  public ForeignKey getForeignKeyFromTo(RelId from_relid,        // Required
                                        RelId to_relid,          // Required
                                        Set<String> field_names, // Optional
                                        ForeignKeyScope inclusion_scope) // Required
  {
      final Set<String> normd_fk_field_names = field_names != null ? normalizeNames(field_names) : null;

      ForeignKey sought_fk = null;
      for(ForeignKey fk: getForeignKeysFromTo(from_relid, to_relid, inclusion_scope))
      {
          if ( normd_fk_field_names == null || fk.sourceFieldNamesSetEqualsNormalizedNamesSet(normd_fk_field_names) )
          {
              if ( sought_fk != null ) // already found an fk satisfying requirements?
                  throw new IllegalArgumentException("Child table " + from_relid +
                                                     " has multiple foreign keys to parent table " + to_relid +
                                                     (field_names != null ? " with the same specified source field set."
                                                                          : " and no foreign key field names were specified to disambiguate."));

              sought_fk = fk;

              // No breaking from the loop here, so case that multiple fk's satisfy requirements can be detected.
          }
      }

      return sought_fk;
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

  public <E> Map<String,E> normalizeNameKeys(Map<String,E> map_with_identifier_keys)
  {
      Map<String,E> res = new HashMap<String,E>();

      for(Map.Entry<String,E> entry: map_with_identifier_keys.entrySet())
      {
          res.put(normalizeDatabaseId(entry.getKey()), entry.getValue());
      }

      return res;
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
  // Sorting for deterministic output

  private List<RelMetaData> sortedMds(List<RelMetaData> rel_mds)
  {
      List<RelMetaData> rmds = new ArrayList<RelMetaData>(rel_mds);

      Collections.sort(rmds, new Comparator<RelMetaData>() {
        @Override
        public int compare(RelMetaData rmd1, RelMetaData rmd2)
        {
            return rmd1.getRelationId().getIdString().compareTo(rmd2.getRelationId().getIdString());
        }
      });

      return Collections.unmodifiableList(rmds);
  }

  /** Return a new copy of the input list, with its foreign keys sorted by source and target relation names and source and target field names. */
  private List<ForeignKey> sortedFks(List<ForeignKey> foreignKeys)
  {
      List<ForeignKey> fks = new ArrayList<ForeignKey>(foreignKeys);

      Collections.sort(fks, new Comparator<ForeignKey>() {
        @Override
        public int compare(ForeignKey fk1, ForeignKey fk2)
        {
            int src_rel_comp = fk1.getSourceRelationId().getIdString().compareTo(fk2.getSourceRelationId().getIdString());
            if ( src_rel_comp != 0 )
                return src_rel_comp;

            int tgt_rel_comp = fk1.getTargetRelationId().getIdString().compareTo(fk2.getTargetRelationId().getIdString());
            if ( tgt_rel_comp != 0 )
                return tgt_rel_comp;

            int src_fields_comp = compareStringListsLexicographically(fk1.getSourceFieldNames(), fk2.getSourceFieldNames());

            if ( src_fields_comp != 0 )
                return src_fields_comp;
            else
                return compareStringListsLexicographically(fk1.getTargetFieldNames(), fk2.getTargetFieldNames());
        }
      });

      return Collections.unmodifiableList(fks);
  }

  private int compareStringListsLexicographically(List<String> strs_1, List<String> strs_2)
  {
      int common_count = Math.min(strs_1.size(), strs_2.size());

      for(int i=0; i<common_count; ++i)
      {
          int comp = strs_1.get(i).compareTo(strs_2.get(i));
          if ( comp != 0 )
              return comp;
      }

      return strs_1.size() < strs_2.size() ? -1
                 : strs_1.size() > strs_2.size() ? 1
                 : 0;
  }

  // Sorting for deterministic output
  /////////////////////////////////////////////////////////


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
      JAXBContext context = JAXBContext.newInstance(getClass());
      Marshaller m = context.createMarshaller();
      m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
      m.marshal(this, os);
      os.flush();
  }

  public static DBMD readXML(InputStream is, boolean close_stream) throws JAXBException, IOException
  {
      Unmarshaller u = JAXBContext.newInstance(DBMD.class).createUnmarshaller();
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
