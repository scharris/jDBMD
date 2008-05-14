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


import java.sql.*;
import java.util.*;

import com.github.scharris.db_metadata.RelationMetaData.RelationType;


public class DBMetaData {
    
    
    public enum CaseSensitivity { INSENSITIVE_STORED_LOWER, INSENSITIVE_STORED_UPPER, INSENSITIVE_STORED_MIXED, SENSITIVE }
    
    
    public static Map<RelationID,RelationType> fetchRelationIDsAndTypes(Connection conn,
                                                                        String schema,
                                                                        boolean incl_tables,
                                                                        boolean incl_views) throws SQLException
    {
        return fetchRelationIDsAndTypes(conn.getMetaData(),
                                        schema,
                                        incl_tables,
                                        incl_views);
    }
    
    public static Map<RelationID,RelationType> fetchRelationIDsAndTypes(DatabaseMetaData dbmd,
                                                                        String schema,
                                                                        boolean incl_tables,
                                                                        boolean incl_views) throws SQLException
    {
        Map<RelationID,RelationType> relid_to_reltype = new HashMap<RelationID,RelationType>();
        
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
            relid_to_reltype.put(new RelationID(rs.getString(1),
                                                rs.getString(2),
                                                rs.getString(3)),
                                 rs.getString(4).toLowerCase().equals("table") ? RelationType.Table : RelationType.View);
        }

        return relid_to_reltype;
    }
    
    public static List<RelationMetaData> fetchRelationMetaDatas(Map<RelationID,RelationType> include_rel_ids_and_types, // relids to include and their types
                                                                String schema,
                                                                Connection conn) throws SQLException
    {
        return fetchRelationMetaDatas(include_rel_ids_and_types,
                                      schema,
                                      conn.getMetaData());
    }
    
    public static List<RelationMetaData> fetchRelationMetaDatas(Map<RelationID,RelationType> include_rel_ids_and_types, // relids to include and their types
                                                                String schema,
                                                                DatabaseMetaData dbmd) throws SQLException
    {
        ResultSet pk_rs = null;
        ResultSet cols_rs = null;

        try
        {
            cols_rs = dbmd.getColumns(null, schema, "%", "%");
            
            List<RelationMetaData> rel_mds = new ArrayList<RelationMetaData>();
            RelationMetaData accum_rel_md = null;
                        
            while(cols_rs.next())
            {
                RelationID rel_id = new RelationID(cols_rs.getString(1), cols_rs.getString(2), cols_rs.getString(3));

                RelationType rel_type = include_rel_ids_and_types.get(rel_id);
                
                if ( rel_type != null )
                {
                    Field f = makeField(cols_rs, dbmd);
                    
                    // Relation changed ?
                    if ( accum_rel_md == null || !rel_id.equals(accum_rel_md.relationID()) )
                    {
                        // done with the old one if any
                        if ( accum_rel_md != null ) 
                            rel_mds.add(accum_rel_md);
                    
                        accum_rel_md = new RelationMetaData(rel_id, rel_type, new ArrayList<Field>());
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
    
    public static List<FkLink> fetchForeignKeyLinks(String schema, Connection conn) throws SQLException
    {
        return fetchForeignKeyLinks(schema, conn.getMetaData());
    }
    
    public static List<FkLink> fetchForeignKeyLinks(String schema, DatabaseMetaData dbmd) throws SQLException
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

    
    protected static Field makeField(ResultSet cols_rs, DatabaseMetaData dbmd) throws SQLException
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
    
    public static CaseSensitivity getDbCaseSensitivity(Connection conn) throws SQLException
    {
        return getDbCaseSensitivity(conn.getMetaData());
    }
    
    public static CaseSensitivity getDbCaseSensitivity(DatabaseMetaData dbmd) throws SQLException
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
    
    public static String normalizeDatabaseIdentifier(String id, CaseSensitivity case_sens)
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
    
    public static Integer getInteger(ResultSet rs, int cnum) throws SQLException
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

}







