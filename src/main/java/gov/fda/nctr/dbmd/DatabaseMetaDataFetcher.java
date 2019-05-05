package gov.fda.nctr.dbmd;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.sql.*;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Pattern;

import gov.fda.nctr.dbmd.RelMetaData.RelType;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toMap;


public class DatabaseMetaDataFetcher {

    public enum DateMapping { DATES_AS_DRIVER_REPORTED, DATES_AS_TIMESTAMPS, DATES_AS_DATES }

    private DateMapping dateMapping;


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


    public DBMD fetchMetaData
        (
            Connection conn,
            Optional<String> schema,
            boolean includeTables,
            boolean includeViews,
            boolean includeFks,
            Pattern excludeRelsPattern
        )
        throws SQLException
    {
        return
            fetchMetaData(
                conn.getMetaData(),
                schema,
                includeTables,
                includeViews,
                includeFks,
                excludeRelsPattern
            );
    }

    public DBMD fetchMetaData
        (
            DatabaseMetaData dbmd,
            Optional<String> schema,
            boolean includeTables,
            boolean includeViews,
            boolean includeFks,
            Pattern excludeRelsPattern
        )
        throws SQLException
    {
        CaseSensitivity caseSens = getDatabaseCaseSensitivity(dbmd);

        Optional<String> normdSchema = schema.map(s -> normalizeDatabaseIdentifier(s, caseSens));

        List<RelDescr> relDescrs = fetchRelationDescriptions(dbmd, normdSchema, includeTables, includeViews, excludeRelsPattern);

        List<RelMetaData> relMds = fetchRelationMetaDatas(relDescrs, normdSchema, dbmd);

        List<ForeignKey> fks = includeFks ? fetchForeignKeys(normdSchema, dbmd, excludeRelsPattern) : emptyList();

        String dbmsName = dbmd.getDatabaseProductName();
        String dbmsVer = dbmd.getDatabaseProductVersion();
        int dbmsMajorVer = dbmd.getDatabaseMajorVersion();
        int dbmsMinorVer = dbmd.getDatabaseMinorVersion();

        return
            new DBMD(
                normdSchema,
                relMds,
                fks,
                caseSens,
                dbmsName,
                dbmsVer,
                dbmsMajorVer,
                dbmsMinorVer
            );
    }


    public List<RelDescr> fetchRelationDescriptions
        (
            Connection conn,
            Optional<String> schema,
            boolean includeTables,
            boolean includeViews,
            Pattern excludeRelsPattern
        )
        throws SQLException
    {
        return
            fetchRelationDescriptions(
                conn.getMetaData(),
                schema,
                includeTables,
                includeViews,
                excludeRelsPattern
            );
    }

    public List<RelDescr> fetchRelationDescriptions
        (
            DatabaseMetaData dbmd,
            Optional<String> schema,
            boolean includeTables,
            boolean includeViews,
            Pattern excludeRelsPattern
        )
        throws SQLException
    {
        List<RelDescr> relDescrs = new ArrayList<>();

        Set<String> relTypes = new HashSet<>();
        if ( includeTables )
            relTypes.add("TABLE");
        if ( includeViews )
            relTypes.add("VIEW");

        ResultSet rs = dbmd.getTables(null, schema.orElse(null), null, relTypes.toArray(new String[relTypes.size()]));

        while ( rs.next() )
        {
            Optional<String> cat = optn(rs.getString("TABLE_CAT"));
            Optional<String> relSchema = optn(rs.getString("TABLE_SCHEM"));
            String relName = rs.getString("TABLE_NAME");

            RelId relId = new RelId(cat, relSchema, relName);

            if ( excludeRelsPattern == null || !excludeRelsPattern.matcher(relId.getIdString()).matches() )
            {
                RelType relType = rs.getString("TABLE_TYPE").toLowerCase().equals("table") ? RelType.Table : RelType.View;

                relDescrs.add(new RelDescr(relId, relType, optn(rs.getString("REMARKS"))));
            }
        }

        return relDescrs;
    }

    public List<RelMetaData> fetchRelationMetaDatas
        (
            List<RelDescr> relDescrs, // descriptions of relations to include
            Optional<String> schema,
            Connection conn
        )
        throws SQLException
    {
        return fetchRelationMetaDatas(relDescrs, schema, conn.getMetaData());
    }

    public List<RelMetaData> fetchRelationMetaDatas
        (
            List<RelDescr> relDescrs, // descriptions of relations to include
            Optional<String> schema,
            DatabaseMetaData dbmd
        )
        throws SQLException
    {
        Map<RelId,RelDescr> relDescrsByRelId = relDescrs.stream().collect(toMap(RelDescr::getRelationId, Function.identity()));

        try ( ResultSet colsRS = dbmd.getColumns(null, schema.orElse(null), "%", "%") )
        {
            List<RelMetaData> relMds = new ArrayList<>();
            RelMetaData accumRelMd = null;

            while ( colsRS.next() )
            {
                Optional<String> cat = optn(colsRS.getString("TABLE_CAT"));
                Optional<String> relSchema = optn(colsRS.getString("TABLE_SCHEM"));
                String relName = colsRS.getString("TABLE_NAME");

                RelId relId = new RelId(cat, relSchema, relName);

                RelDescr relDescr = relDescrsByRelId.get(relId); // may be null if this relation is not in the passed relDescrs list.

                if ( relDescr != null && relDescr.getRelationType() != null )
                {
                    Field f = makeField(colsRS, dbmd);

                    // Relation changed ?
                    if ( accumRelMd == null || !relId.equals(accumRelMd.getRelationId()) )
                    {
                        // done with the old one if any
                        if ( accumRelMd != null )
                            relMds.add(accumRelMd);

                        accumRelMd = new RelMetaData(relId, relDescr.getRelationType(), relDescr.getRelationComment(), emptyList());
                    }

                    accumRelMd.getFields().add(f);
                }
            }

            if ( accumRelMd != null )
                relMds.add(accumRelMd);

            return relMds;
        }
    }

    public List<ForeignKey> fetchForeignKeys
        (
            Optional<String> schema,
            Connection conn,
            Pattern excludeRelsPattern
        )
        throws SQLException
    {
        return fetchForeignKeys(schema, conn.getMetaData(), excludeRelsPattern);
    }

    public List<ForeignKey> fetchForeignKeys
        (
            Optional<String> schema,
            DatabaseMetaData dbmd,
            Pattern excludeRelsPattern
        )
        throws SQLException
    {
        List<ForeignKey> fks = new ArrayList<>();

        try ( ResultSet rs = dbmd.getImportedKeys(null, schema.orElse(null), null) )
        {
            RelId srcRel = null;
            RelId tgtRel = null;
            List<ForeignKey.Component> comps = null;

            while ( rs.next() )
            {
                short compNum = rs.getShort(9);

                if ( compNum == 1 )
                {
                    // If new fk starting then finalize the one we were accumulating
                    if ( comps != null &&
                         (excludeRelsPattern == null ||
                           (!excludeRelsPattern.matcher(srcRel.getIdString()).matches() &&
                            !excludeRelsPattern.matcher(tgtRel.getIdString()).matches())))
                    {
                        fks.add(new ForeignKey(srcRel, tgtRel, comps));
                    }

                    srcRel = new RelId(optn(rs.getString("FKTABLE_CAT")), optn(rs.getString("FKTABLE_SCHEM")), rs.getString("FKTABLE_NAME"));
                    tgtRel = new RelId(optn(rs.getString("PKTABLE_CAT")), optn(rs.getString("PKTABLE_SCHEM")), rs.getString("PKTABLE_NAME"));

                    comps = new ArrayList<>();
                    comps.add(new ForeignKey.Component(rs.getString("FKCOLUMN_NAME"), rs.getString("PKCOLUMN_NAME")));
                }
                else
                {
                    comps.add(new ForeignKey.Component(rs.getString("FKCOLUMN_NAME"), rs.getString("PKCOLUMN_NAME")));
                }
            }

            if ( comps != null )
            {
                if ( excludeRelsPattern == null ||
                     (!excludeRelsPattern.matcher(srcRel.getIdString()).matches() &&
                      !excludeRelsPattern.matcher(tgtRel.getIdString()).matches()) )
                {
                    fks.add(new ForeignKey(srcRel, tgtRel, comps));
                }
            }
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

    public String normalizeDatabaseIdentifier(String id, CaseSensitivity caseSens)
    {
        if ( id.startsWith("\"") && id.endsWith("\"") )
            return id;
        else if ( caseSens == CaseSensitivity.INSENSITIVE_STORED_LOWER )
            return id.toLowerCase();
        else if ( caseSens == CaseSensitivity.INSENSITIVE_STORED_UPPER )
            return id.toUpperCase();
        else
            return id;
    }

    protected static Integer getRSInteger(ResultSet rs, String colName) throws SQLException
    {
        int i = rs.getInt(colName);
        if (rs.wasNull())
            return null;
        else
            return i;
    }

    public static boolean isJdbcTypeNumeric(Integer jdbcType)
    {
           return Field.isJdbcTypeNumeric(jdbcType);
    }

    public static boolean isJdbcTypeChar(Integer jdbcType)
    {
        return Field.isJdbcTypeChar(jdbcType);
    }

    protected Field makeField(ResultSet colsRS, DatabaseMetaData dbmd) throws SQLException
    {
        try ( ResultSet pkRS = dbmd.getPrimaryKeys(colsRS.getString(1), colsRS.getString(2), colsRS.getString(3)) )
        {
            // Fetch the primary key field names and part numbers for this relation
            Map<String, Integer> pkSeqNumsByName = new HashMap<>();

            while (pkRS.next())
                pkSeqNumsByName.put(pkRS.getString(4), pkRS.getInt(5));
            pkRS.close();

            String name = colsRS.getString("COLUMN_NAME");
            Integer typeCode = getRSInteger(colsRS, "DATA_TYPE");
            String dbNativeTypeName = colsRS.getString("TYPE_NAME");

            // Handle special cases/conversions for the type code.
            if ( typeCode == Types.DATE || typeCode == Types.TIMESTAMP )
                typeCode = getTypeCodeForDateOrTimestampColumn(typeCode, dbNativeTypeName);
            else if ( "XMLTYPE".equals(dbNativeTypeName)  || "SYS.XMLTYPE".equals(dbNativeTypeName) )
                typeCode = Types.SQLXML; // Oracle uses proprietary "OPAQUE" code of 2007 as of 11.2, should be Types.SQLXML = 2009.

            Integer size = getRSInteger(colsRS, "COLUMN_SIZE");
            Integer length = isJdbcTypeChar(typeCode) ? size : null;
            Integer nullableDb = getRSInteger(colsRS, "NULLABLE");
            Boolean nullable =
                (nullableDb == ResultSetMetaData.columnNoNulls) ?
                    Boolean.FALSE
                    : (nullableDb == ResultSetMetaData.columnNullable) ? Boolean.TRUE : null;
            Integer fractionalDigits = isJdbcTypeNumeric(typeCode) ? getRSInteger(colsRS, "DECIMAL_DIGITS") : null;
            Integer precision = isJdbcTypeNumeric(typeCode) ? size : null;
            Integer radix = isJdbcTypeNumeric(typeCode) ? getRSInteger(colsRS, "NUM_PREC_RADIX") : null;
            Integer pkPartNum = pkSeqNumsByName.get(name);
            String comment = colsRS.getString("REMARKS");

            return
                new Field(
                    name,
                    typeCode,
                    dbNativeTypeName,
                    length,
                    precision,
                    fractionalDigits,
                    radix,
                    nullable,
                    pkPartNum,
                    comment
                );
        }
    }

    private int getTypeCodeForDateOrTimestampColumn
        (
            int driverReportedTypeCode,
            String dbNativeType
        )
    {
        String dbNativeTypeUc = dbNativeType != null ? dbNativeType.toUpperCase() : null;

        if ( "DATE".equals(dbNativeTypeUc) )
        {
            if ( dateMapping == DateMapping.DATES_AS_TIMESTAMPS )
                return Types.TIMESTAMP;
            else if ( dateMapping == DateMapping.DATES_AS_DATES )
                return Types.DATE;
        }

        return driverReportedTypeCode;
    }

    // Get the property value for the first contained key, or null.
    private static String getProperty(Properties p, String... keys)
    {
        for ( String key: keys )
        {
            if ( p.containsKey(key) )
                return p.getProperty(key);
        }
        return null;
    }


    public static void main(String[] args) throws Exception
    {
        if ( args.length < 2 )
        {
            System.err.println("Expected arguments: jdbc-properties-file [dbmd-properties-file] output-file");

            System.err.println("jdbc properties file properties:\n  " +
                    "  jdbc-driver-class\n" +
                    "  jdbc-connect-url\n" +
                    "  user\n" +
                    "  password\n");

            System.err.println("dbmd properties file properties:\n  " +
                    "  date-mapping (DATES_AS_DRIVER_REPORTED | DATES_AS_TIMESTAMPS | DATES_AS_DATES)\n" +
                    "  relations-owner (schema name | *any-owners*)\n" +
                    "  exclude-relations-fqname-regex\n");

            System.exit(1);
        }

        int argIx = 0;

        String jdbcPropsFilePath = args[argIx++];
        String dbmdPropsFilePath = args.length == 3 ? args[argIx++] : null;
        String outputFilePath = args[argIx];

        Properties props = new Properties();

        try
        {
            props.load(new FileInputStream(jdbcPropsFilePath));
            if ( dbmdPropsFilePath != null && !jdbcPropsFilePath.equals(dbmdPropsFilePath) )
                props.load(new FileInputStream(dbmdPropsFilePath));

            String connStr = getProperty(props, "jdbc.url", "jdbc-connect-url");
            String driverClassname = getProperty(props, "jdbc.driverClassName", "jdbc-driver-class");
            String user = getProperty(props, "jdbc.username", "user");
            String password = getProperty(props, "jdbc.password", "password");

            String dateMappingStr = props.getProperty("date-mapping");
            DateMapping dateMapping = dateMappingStr != null ? DateMapping.valueOf(dateMappingStr) : DateMapping.DATES_AS_DRIVER_REPORTED;

            Optional<String> relsOwner = optn(props.getProperty("relations-owner"));
            if ( relsOwner.isPresent() && "*any-owners*".equals(relsOwner.get()) )
                relsOwner = Optional.empty();

            String excludeRelsRegex = props.getProperty("exclude-relations-fqname-regex");
            Pattern excludeRelsPattern = excludeRelsRegex != null ? Pattern.compile(excludeRelsRegex) : null;

            if ( connStr == null )
                throw new IllegalArgumentException("No jdbc-connect-url property found in config file.");
            if ( user == null )
                throw new IllegalArgumentException("No user property found in config file.");
            if ( password == null )
                throw new IllegalArgumentException("No password property found in config file.");


            if ( driverClassname != null )
                Class.forName(driverClassname);

            Connection conn = DriverManager.getConnection(connStr, user, password);

            DatabaseMetaDataFetcher dbmdFetcher = new DatabaseMetaDataFetcher(dateMapping);

            DBMD dbmd =
                dbmdFetcher.fetchMetaData(
                    conn.getMetaData(),
                    relsOwner,
                    true,
                    true,
                    true,
                    excludeRelsPattern
                );

            FileOutputStream os = new FileOutputStream(outputFilePath);

            // TODO: Write json repr instead.
//          dbmd.writeXML(os);

            os.close();
        }
        catch(Exception e)
        {
            e.printStackTrace();
            throw e;
        }
    }

    private static <E> Optional<E> optn(E e)
    {
        return Optional.ofNullable(e);
    }
}
