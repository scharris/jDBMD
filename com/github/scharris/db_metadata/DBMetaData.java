/* author: Stephen C. Harris.  Send comments, questions and fixes to gmail.com user steveOfAR.
 * Copyright (c) 2008 Stephen C. Harris
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 *     * Redistributions of source code must retain the above copyright notice,
 *       this list of conditions and the following disclaimer.
 *  
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *  
 *     * Neither the name of author nor the names of its contributors
 *       may be used to endorse or promote products derived from this software
 *       without specific prior written permission.
 *  
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package com.github.scharris.db_metadata;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.sql.*;
import java.util.*;
import org.w3c.dom.*;
import org.w3c.dom.ls.*;
import javax.xml.parsers.*;

import com.github.scharris.db_metadata.RelationMetaData.RelationType;

/* Notes
 *  Oracle idiosyncrasies:
 *     - For Oracle 9 and 10 drivers, pass true for perverse_oracle_driver so that Oracle DATE columns are 
 *       treated as SQL TimeStamps.  Failing to do this will cause errors for programs that take this metadata
 *       seriously.  E.g. attempts to use {d yyyy-mm-dd} syntax for setting the field values will fail.
 *     - Oracle will not include comments in metadata by default.  To enable comment reporting, set the remarksReporting
 *       connection property to true.  
 */


public class DBMetaData {
    
    public enum CaseSensitivity { INSENSITIVE_STORED_LOWER, INSENSITIVE_STORED_UPPER, INSENSITIVE_STORED_MIXED, SENSITIVE }
    
    private boolean allDatesAreTimeStamps;
    
    
    public DBMetaData()
    {
    }
    
    public void setAllDatesAreTimeStamps(boolean perverse_oracle_driver)
    {
        allDatesAreTimeStamps = perverse_oracle_driver;
    }
    
    public Document createMetaDataDOM(DatabaseMetaData dbmd,
                                      String schema,
                                      boolean incl_tables,
                                      boolean incl_views,
                                      boolean incl_fields,
                                      boolean incl_fks) throws SQLException, ParserConfigurationException
    {
        CaseSensitivity case_sens = getDbCaseSensitivity(dbmd);

        schema = normalizeDatabaseIdentifier(schema, case_sens);

        Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();

        Element root_el = (Element)doc.appendChild(doc.createElement("database-metadata"));
        if (schema != null)
            root_el.setAttribute("schema", schema);
        root_el.setAttribute("identifiers-case-sensitivity", case_sens.toString().toLowerCase());

        Node rels_el = root_el.appendChild(doc.createElement("relations"));

        List<RelationDescription> rel_descrs = fetchRelationDescriptions(dbmd, schema, incl_tables, incl_views);

        if (incl_fields)
        {
            for (RelationMetaData rel_md : fetchRelationMetaDatas(rel_descrs, schema, dbmd))
                rels_el.appendChild(makeFullRelationElement(rel_md, doc));
        }
        else
        {
            for (RelationDescription rel_descr : rel_descrs)
                rels_el.appendChild(makeChildlessRelationElement(rel_descr.relationId(),
                                                                 rel_descr.relationType(),
                                                                 rel_descr.relationComment(),
                                                                 doc));
        }

        // Foreign Key Links
        if (incl_fks)
        {
            Element fk_links_el = (Element)root_el.appendChild(doc.createElement("foreign-key-links"));

            for (FkLink fkl : fetchForeignKeyLinks(schema, dbmd))
                fk_links_el.appendChild(makeForeignKeyLinkElement(fkl, doc));
        }

        return doc;
    }
    
    protected Element makeChildlessRelationElement(RelationID rel_id, 
                                                   RelationType rel_type,
                                                   String rel_comment,
                                                   Document doc)
    {
        Element rel_el = doc.createElement("relation");
     
        rel_el.setAttribute("type", rel_type.toString().toLowerCase());
        
        rel_el.setAttribute("name", rel_id.name());
                
        if ( rel_id.schema() != null )
            rel_el.setAttribute("schema", rel_id.schema());
                
        if ( rel_id.catalog() != null )
            rel_el.setAttribute("catalog", rel_id.catalog());
                
        rel_el.setAttribute("id", "r:" + rel_id.id());
        
        if ( rel_comment != null )
            rel_el.setAttribute("comment", rel_comment);
        
        return rel_el;
    }
    
    
    protected Element makeFullRelationElement(RelationMetaData rel_md, 
                                              Document doc)
    {
        Element rel_el = makeChildlessRelationElement(rel_md.relationID(), rel_md.relationType(), rel_md.relationComment(), doc);
        
        for(Field f: rel_md.fields())
            rel_el.appendChild(makeFieldElement(f, doc));
        
        return rel_el;
    }
    
    protected Element makeFieldElement(Field f,
                                       Document doc)
    {
        Element field_el = doc.createElement("field");
        
        field_el.setAttribute("name", f.name());
        
        RelationID rel_id = f.relationID();
        field_el.setAttribute("id", "f:" + rel_id.id() + "." + f.name().toLowerCase());

        Element type_el = doc.createElement("type");
        
        appendChildWithText(doc, type_el, "database-type", f.dbTypeName());
        appendChildWithText(doc, type_el, "jdbc-type-code", String.valueOf(f.jdbcTypeCode())); 
        appendChildWithText(doc, type_el, "jdbc-type-text", DBMetaData.jdbcTypeToString(f.jdbcTypeCode())); 
        if ( f.length() != null )
            appendChildWithText(doc, type_el, "max-chars", String.valueOf(f.length()));
        if ( f.precision() != null )
            appendChildWithText(doc, type_el, "precision", String.valueOf(f.precision()));
        if ( f.fractionalDigits() != null )
            appendChildWithText(doc, type_el, "scale", String.valueOf(f.fractionalDigits()));
        if ( f.radix() != null )
            appendChildWithText(doc, type_el, "radix", String.valueOf(f.radix()));
        
        field_el.appendChild(type_el);
        
        appendChildWithText(doc, field_el, "nullable", (f.isNullable() == null ? "unknown" : f.isNullable().toString()));

        if ( f.pkPartNum() != null )
            appendChildWithText(doc, field_el, "primary-key-part", String.valueOf(f.pkPartNum()));

        if ( f.comment() != null )
            appendChildWithText(doc, field_el, "comment", f.comment());
        
        return field_el;
    }
    
    
    protected Element makeForeignKeyLinkElement(FkLink l,
                                                Document doc)
    {
        Element link_el = doc.createElement("link");
       
        Element src_rel_el = (Element)link_el.appendChild(doc.createElement("referencing-relation"));

        src_rel_el.setAttribute("name", l.srcRel().name());

        if (l.srcRel().schema() != null)
            src_rel_el.setAttribute("schema", l.srcRel().schema());

        if (l.srcRel().catalog() != null)
            src_rel_el.setAttribute("catalog", l.srcRel().catalog());

        src_rel_el.setAttribute("rel-id", "r:" + new RelationID(l.srcRel.catalog(), l.srcRel.schema(), l.srcRel.name()).id());

        Element tgt_rel_el = (Element)link_el.appendChild(doc.createElement("referenced-relation"));
        
        tgt_rel_el.setAttribute("name", l.tgtRel().name());

        if (l.tgtRel().schema() != null)
            tgt_rel_el.setAttribute("schema", l.tgtRel().schema());

        if (l.tgtRel().catalog() != null)
            tgt_rel_el.setAttribute("catalog", l.tgtRel().catalog());
        
        tgt_rel_el.setAttribute("rel-id", "r:" + new RelationID(l.tgtRel.catalog(), l.tgtRel.schema(), l.tgtRel.name()).id());

        for(FkComp comp: l.fkComps())
        {
            Element match_el = (Element)link_el.appendChild(doc.createElement("match"));
            match_el.setAttribute("fk-field", comp.fkFieldName());
            match_el.setAttribute("pk-field", comp.pkFieldName());
        }

       return link_el;
    }
    
    
    
    public List<RelationDescription> fetchRelationDescriptions(Connection conn,
                                                               String schema,
                                                               boolean incl_tables,
                                                               boolean incl_views) throws SQLException
    {
        return fetchRelationDescriptions(conn.getMetaData(),
                                         schema,
                                         incl_tables,
                                         incl_views);
    }
    
    public List<RelationDescription> fetchRelationDescriptions(DatabaseMetaData dbmd,
                                                               String schema,
                                                               boolean incl_tables,
                                                               boolean incl_views) throws SQLException
    {
        List<RelationDescription> rel_descrs = new ArrayList<RelationDescription>();
        
        Set<String> rel_types = new HashSet<String>();
        if ( incl_tables )
            rel_types.add("TABLE");
        if ( incl_views )
            rel_types.add("VIEW");

        ResultSet rs = dbmd.getTables(null, // catalog
                                      schema,
                                      null,
                                      rel_types.toArray(new String[rel_types.size()]));

        while (rs.next())
        {
            rel_descrs.add(new RelationDescription(new RelationID(rs.getString(1),rs.getString(2),rs.getString(3)),
                                                  rs.getString(4).toLowerCase().equals("table") ? RelationType.Table : RelationType.View,
                                                  rs.getString(5)));
        }

        return rel_descrs;
    }
    
    public List<RelationMetaData> fetchRelationMetaDatas(List<RelationDescription> rel_descrs, // descriptions of relations to include
                                                         String schema,
                                                         Connection conn) throws SQLException
    {
        return fetchRelationMetaDatas(rel_descrs,
                                      schema,
                                      conn.getMetaData());
    }
    
    public List<RelationMetaData> fetchRelationMetaDatas(List<RelationDescription> rel_descrs, // descriptions of relations to include
                                                         String schema,
                                                         DatabaseMetaData dbmd) throws SQLException
    {
        ResultSet pk_rs = null;
        ResultSet cols_rs = null;

        Map<RelationID,RelationDescription> rel_descrs_by_relid = new HashMap<RelationID,RelationDescription>();
        for(RelationDescription rel_descr: rel_descrs)
            rel_descrs_by_relid.put(rel_descr.relationId(), rel_descr);
        
        try
        {
            cols_rs = dbmd.getColumns(null, schema, "%", "%");
            
            List<RelationMetaData> rel_mds = new ArrayList<RelationMetaData>();
            RelationMetaData accum_rel_md = null;
                        
            while(cols_rs.next())
            {
                RelationID rel_id = new RelationID(cols_rs.getString(1), cols_rs.getString(2), cols_rs.getString(3));

                RelationDescription rel_descr = rel_descrs_by_relid.get(rel_id);
                
                if ( rel_descr.relationType() != null )
                {
                    Field f = makeField(cols_rs, dbmd);
                    
                    // Relation changed ?
                    if ( accum_rel_md == null || !rel_id.equals(accum_rel_md.relationID()) )
                    {
                        // done with the old one if any
                        if ( accum_rel_md != null ) 
                            rel_mds.add(accum_rel_md);
                    
                        accum_rel_md = new RelationMetaData(rel_id, 
                                                            rel_descr.relationType(),
                                                            rel_descr.relationComment(),
                                                            new ArrayList<Field>());
                    }
                        
                    accum_rel_md.fields().add(f);
                }
            }
            
            if ( accum_rel_md != null )
                rel_mds.add(accum_rel_md);

            return rel_mds;
        }
        finally
        {
            try
            {
                if (pk_rs != null)
                    pk_rs.close();
                if (cols_rs != null)
                    cols_rs.close();
            }
            catch (SQLException se)
            {
                System.err.println("Couldn't close database resources: " + se);
            }
        }
    }
    
    public List<FkLink> fetchForeignKeyLinks(String schema, Connection conn) throws SQLException
    {
        return fetchForeignKeyLinks(schema, conn.getMetaData());
    }
    
    public List<FkLink> fetchForeignKeyLinks(String schema, DatabaseMetaData dbmd) throws SQLException
    {
        List<FkLink> links = new ArrayList<FkLink>();

        ResultSet fk_rs = dbmd.getImportedKeys(null, schema, null);

        try
        {
            RelationID src_rel = null;
            RelationID tgt_rel = null;
            List<FkComp> comps = null;

            while (fk_rs.next())
            {
                short comp_num = fk_rs.getShort(9);

                if (comp_num == 1)
                {
                    // If new fk starting then finalize the one we were accumulating
                    if (comps != null)
                        links.add(new FkLink(src_rel, tgt_rel, comps));

                    src_rel = new RelationID(fk_rs.getString(5), fk_rs.getString(6), fk_rs.getString(7));
                    tgt_rel = new RelationID(fk_rs.getString(1), fk_rs.getString(2), fk_rs.getString(3));
                    
                    comps = new ArrayList<FkComp>();
                    comps.add(new FkComp(fk_rs.getString(8), fk_rs.getString(4)));
                }
                else
                {
                    comps.add(new FkComp(fk_rs.getString(8), fk_rs.getString(4)));
                }
            }

            if (comps != null)
                links.add(new FkLink(src_rel, tgt_rel, comps));
        }
        finally
        {
            fk_rs.close();
        }

        return links;
    }

    
    protected Field makeField(ResultSet cols_rs, DatabaseMetaData dbmd) throws SQLException
    {
        ResultSet pk_rs = null;

        try
        {
            // Fetch the primary key field names and part numbers for this relation
            Map<String, Integer> pkseqnums_by_name = new HashMap<String, Integer>();
            pk_rs = dbmd.getPrimaryKeys(cols_rs.getString(1), cols_rs.getString(2), cols_rs.getString(3));
            while (pk_rs.next())
                pkseqnums_by_name.put(pk_rs.getString(4), pk_rs.getInt(5));
            pk_rs.close();

            String name = cols_rs.getString(4);
            int type_code = getInteger(cols_rs, 5);
            
            /*  The allDatesAreTimeStamps workaround is for Oracle 9 and 10 drivers (have not tested 11 yet), which report
             *  oracle DATE columns as SQL Date columns, when they are really SQL Timestamps.  Considering the column as a
             *  SQL Date would cause a failure if e.g. an attempt is made to insert a {d YYYY-mm-dd} standard jdbc escaped
             *  SQL Date value into the column. 
             */ 
            if ( type_code == Types.DATE && allDatesAreTimeStamps ) 
                type_code = Types.TIMESTAMP;
            
            int size = getInteger(cols_rs, 7);
            int nullable = getInteger(cols_rs, 11);
            
            return new Field(new RelationID(cols_rs.getString(1), cols_rs.getString(2), cols_rs.getString(3)),
                             name,
                             type_code,
                             cols_rs.getString(6), // type name
                             isJdbcTypeChar(type_code) ? size : null, // length
                             isJdbcTypeNumeric(type_code) ? size : null, // precision
                             isJdbcTypeNumeric(type_code) ? getInteger(cols_rs, 9) : null, // fractional_digits
                             isJdbcTypeNumeric(type_code) ? getInteger(cols_rs, 10) : null, // radix
                             (nullable == ResultSetMetaData.columnNoNulls) ? Boolean.FALSE 
                                           : (nullable == ResultSetMetaData.columnNullable) ? Boolean.TRUE : null,
                             pkseqnums_by_name.get(name), // pk part num
                             cols_rs.getString(12)); // comment
        }
        finally
        {
            if (pk_rs != null)
                pk_rs.close();
        }
    }
    
    public CaseSensitivity getDbCaseSensitivity(Connection conn) throws SQLException
    {
        return getDbCaseSensitivity(conn.getMetaData());
    }
    
    public CaseSensitivity getDbCaseSensitivity(DatabaseMetaData dbmd) throws SQLException
    {
        if ( dbmd.storesLowerCaseIdentifiers() )
            return CaseSensitivity.INSENSITIVE_STORED_LOWER;
        else if ( dbmd.storesUpperCaseIdentifiers() )
            return CaseSensitivity.INSENSITIVE_STORED_UPPER;
        else if ( dbmd.storesMixedCaseIdentifiers() )
            return CaseSensitivity.INSENSITIVE_STORED_MIXED;
        else
            return CaseSensitivity.SENSITIVE;
    }
    
    public String normalizeDatabaseIdentifier(String id, CaseSensitivity case_sens)
    {
        if (id.equals(""))
            return null;
        else if ( id.equals("<none>") )
            return "";
        else if ( case_sens == CaseSensitivity.INSENSITIVE_STORED_LOWER )
            return id.toLowerCase();
        else if ( case_sens == CaseSensitivity.INSENSITIVE_STORED_UPPER )
            return id.toUpperCase();
        else
            return id;
    }
    
    protected static void appendChildWithText(Document doc, Node node, String child_name, String child_text)
    {
        Element child = doc.createElement(child_name);
        child.setTextContent(child_text);
        node.appendChild(child);
    }
    
    protected static Integer getInteger(ResultSet rs, int cnum) throws SQLException
    {
        int i = rs.getInt(cnum);
        if (rs.wasNull())
            return null;
        else
            return i;
    }
    
    public static boolean isJdbcTypeNumeric(int jdbc_type)
    {
        switch (jdbc_type)
        {
        case Types.TINYINT:
        case Types.SMALLINT:
        case Types.INTEGER:
        case Types.BIGINT:
        case Types.FLOAT:
        case Types.REAL:
        case Types.DOUBLE:
        case Types.DECIMAL:
        case Types.NUMERIC:
            return true;
        default:
            return false;
        }
    }
    
    public static boolean isJdbcTypeChar(int jdbc_type)
    {
        switch (jdbc_type)
        {
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
            return true;
        default:
            return false;
        }
    }
    
    public static String jdbcTypeToString(int jdbc_type)
    {
        switch (jdbc_type)
        {
        case Types.TINYINT:
            return "TINYINT";
        case Types.SMALLINT:
            return "SMALLINT";
        case Types.INTEGER:
            return "INTEGER";
        case Types.BIGINT:
            return "BIGINT";
        case Types.FLOAT:
            return "FLOAT";
        case Types.REAL:
            return "REAL";
        case Types.DOUBLE:
            return "DOUBLE";
        case Types.DECIMAL:
            return "DECIMAL";
        case Types.NUMERIC:
            return "NUMERIC";
        case Types.CHAR:
            return "CHAR";
        case Types.VARCHAR:
            return "VARCHAR";
        case Types.LONGVARCHAR:
            return "LONGVARCHAR";
        case Types.BIT:
            return "BIT";
        case Types.DATE:
            return "DATE";
        case Types.TIME:
            return "TIME";
        case Types.TIMESTAMP:
            return "TIMESTAMP";
        case Types.BINARY:
            return "BINARY";
        case Types.VARBINARY:
            return "VARBINARY";
        case Types.LONGVARBINARY:
            return "LONGVARBINARY";
        case Types.NULL:
            return "NULL";
        case Types.OTHER:
            return "OTHER";
        case Types.JAVA_OBJECT:
            return "JAVA_OBJECT";
        case Types.DISTINCT:
            return "DISTINCT";
        case Types.STRUCT:
            return "STRUCT";
        case Types.ARRAY:
            return "ARRAY";
        case Types.REF:
            return "REF";
        case Types.DATALINK:
            return "DATALINK";
        case Types.BOOLEAN:
            return "BOOLEAN";
        case Types.BLOB:
            return "BLOB";
        case Types.CLOB:
            return "CLOB";
        }
        return "unknown[" + jdbc_type + "]";
    }

    
    public static void main(String[] args) // schema connection-properties-file output-file 
    {
        if ( args.length < 3 )
        {
            System.err.println("Expected arguments: schema connection-properties-file output-file");
            System.exit(1);
        }
        
        int arg_ix = 0;
        String schema = args[arg_ix++];
        String props_file_path = args[arg_ix++];
        String output_file_path = args[arg_ix++];

        Properties props = new Properties();
        
        Connection conn = null;
        
        try
        {
            props.load(new FileInputStream(props_file_path));
            
            String conn_str = props.getProperty("jdbc-connect-url");
            String driver_classname = props.getProperty("jdbc-driver-class");
            String user = props.getProperty("user");
            String password = props.getProperty("password");
            
            if ( conn_str == null )
                throw new IllegalArgumentException("No jdbc-connect-url property found in config file.");
            if ( driver_classname == null )
                throw new IllegalArgumentException("No jdbc-driver-class property found in config file.");
            if ( user == null )
                throw new IllegalArgumentException("No user property found in config file.");
            if ( password == null )
                throw new IllegalArgumentException("No password property found in config file.");
            
            
            Class.forName(driver_classname);
            conn = DriverManager.getConnection(conn_str, user, password);
            
            DBMetaData dbmd = new DBMetaData();
            
            Document doc = dbmd.createMetaDataDOM(conn.getMetaData(), schema, true, true, true, true);
            
            dbmd.writeDocument(doc, new File(output_file_path));
            
            System.exit(0);
        }
        catch(Exception e)
        {
            System.err.println("Error: " + e.getMessage());
            System.exit(2);
        }
    }

    
    public void writeDocument(Document doc, File out_file) throws FileNotFoundException 
    {
        DOMImplementationLS dom_impl_ls = (DOMImplementationLS)doc.getImplementation().getFeature("LS", "3.0");
        
        LSSerializer ls_ser = dom_impl_ls.createLSSerializer();

        LSOutput ls_out = dom_impl_ls.createLSOutput();
        ls_out.setByteStream(new FileOutputStream(out_file));

        ls_ser.write(doc, ls_out);
    }

    
    static class RelationDescription {
    
        private RelationID relId;
        private RelationType relType;
        private String relComment;
    
        public RelationDescription(RelationID relId, RelationType relType, String relComment)
        {
            super();
            this.relId = relId;
            this.relType = relType;
            this.relComment = relComment;
        }

        public RelationID relationId()
        {
            return relId;
        }

        public RelationType relationType()
        {
            return relType;
        }

        public String relationComment()
        {
            return relComment;
        }
    }

}





