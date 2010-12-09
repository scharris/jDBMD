package gov.fda.nctr.dbmd;



import gov.fda.nctr.dbmd.DBMD.CaseSensitivity;
import gov.fda.nctr.dbmd.RelMetaData.RelType;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.sql.*;
import java.util.*;


public class DatabaseMetaDataFetcher {
    
	public static enum DateMapping { DATES_AS_DRIVER_REPORTED, DATES_AS_TIMESTAMPS, DATES_AS_DATES }
	
	DateMapping dateMapping;
	
    
    public DatabaseMetaDataFetcher()
    {
    	this(DateMapping.DATES_AS_DRIVER_REPORTED);
    }
    
    public DatabaseMetaDataFetcher(DateMapping mapping)
    {
    	this.dateMapping = mapping;
    }
    
    
    public void setDateMapping(DateMapping mapping)
    {
    	this.dateMapping = mapping;
    }
    
    
    public DBMD fetchMetaData(Connection conn,
                              String schema,
                              boolean incl_tables,
                              boolean incl_views,
                              boolean incl_fields,
                              boolean incl_fks) throws SQLException
    {
    	return fetchMetaData(conn.getMetaData(),
    	                     schema,
    	                     incl_tables,
    	                     incl_views,
    	                     incl_fields,
    	                     incl_fks);
    }
    

    public DBMD fetchMetaData(DatabaseMetaData dbmd,
                              String schema,
                              boolean incl_tables,
                              boolean incl_views,
                              boolean incl_fields,
                              boolean incl_fks) throws SQLException
    {
        CaseSensitivity case_sens = getDatabaseCaseSensitivity(dbmd);

        schema = normalizeDatabaseIdentifier(schema, case_sens);

        List<RelDescr> rel_descrs = fetchRelationDescriptions(dbmd, schema, incl_tables, incl_views);
        
        List<RelMetaData> rel_mds = incl_fields ? fetchRelationMetaDatas(rel_descrs, schema, dbmd) : null;
        
        List<ForeignKey> fks = fetchForeignKeys(schema, dbmd);
        
        return new DBMD(schema, rel_mds, fks, case_sens);
    }
    
    
    public List<RelDescr> fetchRelationDescriptions(Connection conn,
                                                    String schema,
                                                    boolean incl_tables,
                                                    boolean incl_views) throws SQLException
    {
        return fetchRelationDescriptions(conn.getMetaData(),
                                         schema,
                                         incl_tables,
                                         incl_views);
    }
    
    public List<RelDescr> fetchRelationDescriptions(DatabaseMetaData dbmd,
                                                    String schema,
                                                    boolean incl_tables,
                                                    boolean incl_views) throws SQLException
    {
        List<RelDescr> rel_descrs = new ArrayList<RelDescr>();
        
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
            rel_descrs.add(new RelDescr(new RelId(rs.getString("TABLE_CAT"),rs.getString("TABLE_SCHEM"),rs.getString("TABLE_NAME")),
                                                  rs.getString("TABLE_TYPE").toLowerCase().equals("table") ? RelType.Table : RelType.View,
                                                  rs.getString("REMARKS")));
        }

        return rel_descrs;
    }
    
    public List<RelMetaData> fetchRelationMetaDatas(List<RelDescr> rel_descrs, // descriptions of relations to include
                                                    String schema,
                                                    Connection conn) throws SQLException
    {
        return fetchRelationMetaDatas(rel_descrs,
                                      schema,
                                      conn.getMetaData());
    }
    
    public List<RelMetaData> fetchRelationMetaDatas(List<RelDescr> rel_descrs, // descriptions of relations to include
                                                    String schema,
                                                    DatabaseMetaData dbmd) throws SQLException
    {
        ResultSet pk_rs = null;
        ResultSet cols_rs = null;

        Map<RelId,RelDescr> rel_descrs_by_relid = new HashMap<RelId,RelDescr>();
        for(RelDescr rel_descr: rel_descrs)
            rel_descrs_by_relid.put(rel_descr.getRelationId(), rel_descr);
        
        try
        {
            cols_rs = dbmd.getColumns(null, schema, "%", "%");
            
            List<RelMetaData> rel_mds = new ArrayList<RelMetaData>();
            RelMetaData accum_rel_md = null;
                        
            while(cols_rs.next())
            {
                RelId rel_id = new RelId(cols_rs.getString("TABLE_CAT"), cols_rs.getString("TABLE_SCHEM"), cols_rs.getString("TABLE_NAME"));

                RelDescr rel_descr = rel_descrs_by_relid.get(rel_id);
                
                if ( rel_descr.getRelationType() != null )
                {
                    Field f = makeField(cols_rs, dbmd);
                    
                    // Relation changed ?
                    if ( accum_rel_md == null || !rel_id.equals(accum_rel_md.getRelationId()) )
                    {
                        // done with the old one if any
                        if ( accum_rel_md != null ) 
                            rel_mds.add(accum_rel_md);
                    
                        accum_rel_md = new RelMetaData(rel_id, 
                                                       rel_descr.getRelationType(),
                                                       rel_descr.getRelationComment(),
                                                       new ArrayList<Field>());
                    }
                        
                    accum_rel_md.getFields().add(f);
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
    
    public List<ForeignKey> fetchForeignKeys(String schema, Connection conn) throws SQLException
    {
        return fetchForeignKeys(schema, conn.getMetaData());
    }
    
    public List<ForeignKey> fetchForeignKeys(String schema, DatabaseMetaData dbmd) throws SQLException
    {
        List<ForeignKey> fks = new ArrayList<ForeignKey>();

        ResultSet fk_rs = dbmd.getImportedKeys(null, schema, null);

        try
        {
            RelId src_rel = null;
            RelId tgt_rel = null;
            List<ForeignKey.Component> comps = null;

            while (fk_rs.next())
            {
                short comp_num = fk_rs.getShort(9);

                if (comp_num == 1)
                {
                    // If new fk starting then finalize the one we were accumulating
                    if (comps != null)
                        fks.add(new ForeignKey(src_rel, tgt_rel, comps));

                    src_rel = new RelId(fk_rs.getString("FKTABLE_CAT"), fk_rs.getString("FKTABLE_SCHEM"), fk_rs.getString("FKTABLE_NAME"));
                    tgt_rel = new RelId(fk_rs.getString("PKTABLE_CAT"), fk_rs.getString("PKTABLE_SCHEM"), fk_rs.getString("PKTABLE_NAME"));
                    
                    comps = new ArrayList<ForeignKey.Component>();
                    comps.add(new ForeignKey.Component(fk_rs.getString("FKCOLUMN_NAME"), fk_rs.getString("PKCOLUMN_NAME")));
                }
                else
                {
                    comps.add(new ForeignKey.Component(fk_rs.getString("FKCOLUMN_NAME"), fk_rs.getString("PKCOLUMN_NAME")));
                }
            }

            if (comps != null)
                fks.add(new ForeignKey(src_rel, tgt_rel, comps));
        }
        finally
        {
            fk_rs.close();
        }

        return fks;
    }

    
    public CaseSensitivity getDatabaseCaseSensitivity(Connection conn) throws SQLException
    {
        return getDatabaseCaseSensitivity(conn.getMetaData());
    }
    
    public CaseSensitivity getDatabaseCaseSensitivity(DatabaseMetaData dbmd) throws SQLException
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
        if (id == null || id.equals(""))
            return null;
        else if ( id.equals("<none>") )
            return "";
        else if ( id.startsWith("\"") && id.endsWith("\"") )
        	return id;
        else if ( case_sens == CaseSensitivity.INSENSITIVE_STORED_LOWER )
            return id.toLowerCase();
        else if ( case_sens == CaseSensitivity.INSENSITIVE_STORED_UPPER )
            return id.toUpperCase();
        else
            return id;
    }
    
    protected static Integer getRSInteger(ResultSet rs, int cnum) throws SQLException
    {
        int i = rs.getInt(cnum);
        if (rs.wasNull())
            return null;
        else
            return i;
    }
    
    protected static Integer getRSInteger(ResultSet rs, String col_name) throws SQLException
    {
        int i = rs.getInt(col_name);
        if (rs.wasNull())
            return null;
        else
            return i;
    }
    
    public static boolean isJdbcTypeNumeric(Integer jdbc_type)
    {
       	return Field.isJdbcTypeNumeric(jdbc_type);
    }
    
    public static boolean isJdbcTypeChar(Integer jdbc_type)
    {
    	return Field.isJdbcTypeChar(jdbc_type);
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
	
	        String name = cols_rs.getString("COLUMN_NAME");
	        Integer type_code = getRSInteger(cols_rs, "DATA_TYPE");
	        String db_native_type_name = cols_rs.getString("TYPE_NAME");
	        if ( type_code == Types.DATE || type_code == Types.TIMESTAMP )
	        	type_code = getTypeCodeForDateOrTimestampColumn(type_code, db_native_type_name);
	        Integer size = getRSInteger(cols_rs, "COLUMN_SIZE");
	        Integer length = isJdbcTypeChar(type_code) ? size : null;
	        Integer nullable_db = getRSInteger(cols_rs, "NULLABLE");
	        Boolean nullable = (nullable_db == ResultSetMetaData.columnNoNulls) ?
	        		Boolean.FALSE 
	        	 : (nullable_db == ResultSetMetaData.columnNullable) ? Boolean.TRUE : null;
	        Integer fractional_digits = isJdbcTypeNumeric(type_code) ? getRSInteger(cols_rs, "DECIMAL_DIGITS") : null;
	        Integer precision = isJdbcTypeNumeric(type_code) ? size : null;
	        Integer radix = isJdbcTypeNumeric(type_code) ? getRSInteger(cols_rs, "NUM_PREC_RADIX") : null;
	        Integer pk_part_num = pkseqnums_by_name.get(name);
	        String comment = cols_rs.getString("REMARKS");
	        
	        return new Field(new RelId(cols_rs.getString("TABLE_CAT"), cols_rs.getString("TABLE_SCHEM"), cols_rs.getString("TABLE_NAME")),
	                         name,
	                         type_code,
	                         db_native_type_name,
	                         length,
	                         precision,
	                         fractional_digits,
	                         radix,
	                         nullable,
	                         pk_part_num,
	                         comment);
	    }
	    finally
	    {
	        if (pk_rs != null)
	            pk_rs.close();
	    }
	}

    private int getTypeCodeForDateOrTimestampColumn(int driver_reported_type_code,
                                                    String db_native_type)
	{
    	String db_native_type_uc = db_native_type != null ? db_native_type.toUpperCase() : null;
    	
    	if ( "DATE".equals(db_native_type_uc) )
    	{
    		if ( dateMapping == DateMapping.DATES_AS_TIMESTAMPS )
    			return Types.TIMESTAMP;
    		else if ( dateMapping == DateMapping.DATES_AS_DATES )
        		return Types.DATE;
    	}

    	return driver_reported_type_code;
	}


	public static void main(String[] args) // schema connection-properties-file output-file 
    {
        if ( args.length < 2 )
        {
            System.err.println("Expected arguments: [-DATES_AS_DRIVER_REPORTED|-DATES_AS_TIMESTAMPS|-DATES_AS_DATES] (schema|*any-owners*) connection-properties-file output-file");
            System.exit(1);
        }
        
        int arg_ix = 0;
        
        DateMapping date_mapping;
        if ( args.length == 4 )
        	date_mapping = DateMapping.valueOf(args[arg_ix++].substring(1));
        else
        	date_mapping = DateMapping.DATES_AS_DRIVER_REPORTED;
        
        String schema = args[arg_ix++];
        if ( schema.equals("*any-owners*") )
        	schema = null;
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
            if ( user == null )
                throw new IllegalArgumentException("No user property found in config file.");
            if ( password == null )
                throw new IllegalArgumentException("No password property found in config file.");
            
            
            if ( driver_classname != null )
            	Class.forName(driver_classname);
            
            conn = DriverManager.getConnection(conn_str, user, password);
            
            DatabaseMetaDataFetcher mdfetcher = new DatabaseMetaDataFetcher(date_mapping);
            
            DBMD md = mdfetcher.fetchMetaData(conn.getMetaData(), schema, true, true, true, true);

            FileOutputStream os = new FileOutputStream(output_file_path);
            
            md.writeXML(os);
            
            os.close();
            
            System.exit(0);
        }
        catch(Exception e)
        {
        	e.printStackTrace();
            System.exit(1);
        }
    }

}
