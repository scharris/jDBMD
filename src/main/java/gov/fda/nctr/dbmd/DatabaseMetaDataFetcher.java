package gov.fda.nctr.dbmd;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.regex.Pattern;

import static gov.fda.nctr.dbmd.RelMetaData.RelType.Table;
import static gov.fda.nctr.dbmd.RelMetaData.RelType.View;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

import gov.fda.nctr.dbmd.RelMetaData.RelType;


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
            Optional<Pattern> excludeRelsPattern
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
            Optional<Pattern> excludeRelsPat
        )
        throws SQLException
    {
        CaseSensitivity caseSens = getDatabaseCaseSensitivity(dbmd);

        Optional<String> nSchema = schema.map(s -> normalizeDatabaseIdentifier(s, caseSens));

        List<RelDescr> relDescrs = fetchRelationDescriptions(dbmd, nSchema, includeTables, includeViews, excludeRelsPat);

        List<RelMetaData> relMds = fetchRelationMetaDatas(relDescrs, nSchema, dbmd);

        List<ForeignKey> fks = includeFks ? fetchForeignKeys(nSchema, dbmd, excludeRelsPat) : emptyList();

        String dbmsName = dbmd.getDatabaseProductName();
        String dbmsVer = dbmd.getDatabaseProductVersion();
        int dbmsMajorVer = dbmd.getDatabaseMajorVersion();
        int dbmsMinorVer = dbmd.getDatabaseMinorVersion();

        return
            new DBMD(
                nSchema,
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
            Optional<Pattern> excludeRelsPattern
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
            Optional<Pattern> excludeRelsPattern
        )
        throws SQLException
    {
        List<RelDescr> relDescrs = new ArrayList<>();

        Set<String> relTypes = new HashSet<>();
        if ( includeTables )
            relTypes.add("TABLE");
        if ( includeViews )
            relTypes.add("VIEW");

        ResultSet rs = dbmd.getTables(null, schema.orElse(null), null, relTypes.toArray(new String[0]));

        while ( rs.next() )
        {
            Optional<String> cat = optn(rs.getString("TABLE_CAT"));
            Optional<String> relSchema = optn(rs.getString("TABLE_SCHEM"));
            String relName = rs.getString("TABLE_NAME");

            RelId relId = new RelId(cat, relSchema, relName);

            if ( !matches(excludeRelsPattern, relId.getIdString()) )
            {
                RelType relType = rs.getString("TABLE_TYPE").toLowerCase().equals("table") ? Table : View;

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
        Map<RelId,RelDescr> relDescrsByRelId = relDescrs.stream().collect(toMap(RelDescr::getRelationId, identity()));

        try ( ResultSet colsRS = dbmd.getColumns(null, schema.orElse(null), "%", "%") )
        {
            List<RelMetaData> relMds = new ArrayList<>();
            RelMetaDataBuilder rmdBldr = null;

            while ( colsRS.next() )
            {
                Optional<String> cat = optn(colsRS.getString("TABLE_CAT"));
                Optional<String> relSchema = optn(colsRS.getString("TABLE_SCHEM"));
                String relName = colsRS.getString("TABLE_NAME");

                RelId relId = new RelId(cat, relSchema, relName);

                RelDescr relDescr = relDescrsByRelId.get(relId);
                if ( relDescr != null ) // Include this relation?
                {
                    Field f = makeField(colsRS, dbmd);

                    // Relation changed ?
                    if ( rmdBldr == null || !relId.equals(rmdBldr.relId) )
                    {
                        // finalize previous if any
                        if ( rmdBldr != null )
                            relMds.add(rmdBldr.build());

                        rmdBldr = new RelMetaDataBuilder(relId, relDescr.getRelationType(), relDescr.getRelationComment());
                    }

                    rmdBldr.addField(f);
                }
            }

            if ( rmdBldr != null )
                relMds.add(rmdBldr.build());

            return relMds;
        }
    }

    public List<ForeignKey> fetchForeignKeys
        (
            Optional<String> schema,
            Connection conn,
            Optional<Pattern> excludeRelsPattern
        )
        throws SQLException
    {
        return fetchForeignKeys(schema, conn.getMetaData(), excludeRelsPattern);
    }

    public List<ForeignKey> fetchForeignKeys
        (
            Optional<String> schema,
            DatabaseMetaData dbmd,
            Optional<Pattern> excludeRelsPattern
        )
        throws SQLException
    {
        List<ForeignKey> fks = new ArrayList<>();

        try ( ResultSet rs = dbmd.getImportedKeys(null, schema.orElse(null), null) )
        {
            FkBuilder fkBldr = null;

            while ( rs.next() )
            {
                short compNum = rs.getShort(9);

                if ( compNum == 1 ) // starting new fk
                {
                    // Finalize previous fk if any.
                    if ( fkBldr != null && fkBldr.neitherRelMatches(excludeRelsPattern) )
                        fks.add(fkBldr.build());

                    fkBldr = new FkBuilder(
                        new RelId(optn(rs.getString("FKTABLE_CAT")),
                                  optn(rs.getString("FKTABLE_SCHEM")),
                                  rs.getString("FKTABLE_NAME")),
                        new RelId(optn(rs.getString("PKTABLE_CAT")),
                                  optn(rs.getString("PKTABLE_SCHEM")),
                                  rs.getString("PKTABLE_NAME"))
                    );
                    fkBldr.addComponent(
                        new ForeignKey.Component(rs.getString("FKCOLUMN_NAME"), rs.getString("PKCOLUMN_NAME"))
                    );
                }
                else // adding another fk component
                {
                    requireNonNull(fkBldr); // because we should have seen a component # 1 before entering here
                    fkBldr.addComponent(
                        new ForeignKey.Component(rs.getString("FKCOLUMN_NAME"), rs.getString("PKCOLUMN_NAME"))
                    );
                }
            }

            if ( fkBldr != null && fkBldr.neitherRelMatches(excludeRelsPattern) )
                fks.add(fkBldr.build());
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

    protected static Optional<Integer> getRSInt(ResultSet rs, String colName) throws SQLException
    {
        int i = rs.getInt(colName);
        return rs.wasNull() ? Optional.empty() : Optional.of(i);
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
            int typeCode = colsRS.getInt("DATA_TYPE");
            String dbType = colsRS.getString("TYPE_NAME");

            // Handle special cases/conversions for the type code.
            if ( typeCode == Types.DATE || typeCode == Types.TIMESTAMP )
                typeCode = getTypeCodeForDateOrTimestampColumn(typeCode, dbType);
            else if ( "XMLTYPE".equals(dbType)  || "SYS.XMLTYPE".equals(dbType) )
                // Oracle uses proprietary "OPAQUE" code of 2007 as of 11.2, should be Types.SQLXML = 2009.
                typeCode = Types.SQLXML;

            Optional<Integer> size = getRSInt(colsRS, "COLUMN_SIZE");
            Optional<Integer> length = isJdbcTypeChar(typeCode) ? size : Optional.empty();
            Optional<Boolean> nullable = getRSInt(colsRS, "NULLABLE").flatMap(n ->
                n == ResultSetMetaData.columnNullable ? Optional.of(true) :
                n == ResultSetMetaData.columnNoNulls ? Optional.of(false) :
                    Optional.empty()
            );
            Optional<Integer> fracDigs =
                isJdbcTypeNumeric(typeCode) ? getRSInt(colsRS, "DECIMAL_DIGITS") : Optional.empty();
            Optional<Integer> prec = isJdbcTypeNumeric(typeCode) ? size : Optional.empty();
            Optional<Integer> rad =
                isJdbcTypeNumeric(typeCode) ? getRSInt(colsRS, "NUM_PREC_RADIX") : Optional.empty();
            Optional<Integer> pkPart = optn(pkSeqNumsByName.get(name));
            Optional<String> comment = optn(colsRS.getString("REMARKS"));

            return new Field(name, typeCode, dbType, length, prec, fracDigs, rad, nullable, pkPart, comment);
        }
    }

    private int getTypeCodeForDateOrTimestampColumn
        (
            int driverReportedTypeCode,
            String dbNativeType
        )
    {
        String dbNativeTypeUc = dbNativeType.toUpperCase();

        if ( "DATE".equals(dbNativeTypeUc) )
        {
            if ( dateMapping == DateMapping.DATES_AS_TIMESTAMPS )
                return Types.TIMESTAMP;
            else if ( dateMapping == DateMapping.DATES_AS_DATES )
                return Types.DATE;
        }

        return driverReportedTypeCode;
    }


    /////////////////////////////////////////////////////////
    // auxiliary builder classes

    private static class FkBuilder {

        private RelId srcRel;
        private RelId tgtRel;
        private List<ForeignKey.Component> comps;

        public FkBuilder(RelId srcRel, RelId tgtRel)
        {
            this.srcRel = srcRel;
            this.tgtRel = tgtRel;
            this.comps = new ArrayList<>();
        }

        boolean neitherRelMatches(Optional<Pattern> relIdsPattern)
        {
            return !(matches(relIdsPattern, srcRel.getIdString()) || matches(relIdsPattern, tgtRel.getIdString()));
        }

        ForeignKey build() { return new ForeignKey(srcRel, tgtRel, comps); }

        void addComponent(ForeignKey.Component comp) { comps.add(comp); }
    }

    private static class RelMetaDataBuilder {

        private final RelId relId;

        private final RelMetaData.RelType relType;

        private final Optional<String> relComment;

        private final List<Field> fields;

        public RelMetaDataBuilder
                (
                        RelId relId,
                        RelType relType,
                        Optional<String> relComment
                )
        {
            this.relId = requireNonNull(relId);
            this.relType = requireNonNull(relType);
            this.relComment = requireNonNull(relComment);
            this.fields = new ArrayList<>();
        }

        public void addField(Field f) { fields.add(f); }

        public RelMetaData build()
        {
            return new RelMetaData(relId, relType, relComment, fields);
        }
    }

    // auxiliary builder classes
    /////////////////////////////////////////////////////////


    private static boolean matches(Optional<Pattern> pat, String s)
    {
        return pat.map(p -> p.matcher(s).matches()).orElse(false);
    }

    private static <E> Optional<E> optn(E e)
    {
        return Optional.ofNullable(e);
    }

    // Get the property value for the first contained key if any.
    private static Optional<String> getProperty(Properties p, String... keys)
    {
        for ( String key: keys )
        {
            if ( p.containsKey(key) )
                return Optional.of(p.getProperty(key));
        }
        return Optional.empty();
    }

    private static String requireProperty(Properties p, String... keys)
    {
        return getProperty(p, keys).orElseThrow(() ->
                new RuntimeException("Property " + keys[0] + " is required.")
        );
    }

    private static OutputStream outputStream(String pathOrDash) throws IOException
    {
        if ( "-".equals(pathOrDash) )
            return System.out;
        else
            return new FileOutputStream(pathOrDash);
    }

    private static void printUsage(PrintStream ps)
    {
        ps.println("Expected arguments: jdbc-properties-file [dbmd-properties-file] output-file|-");

        ps.println(
            "jdbc properties file properties:\n  " +
            "  jdbc-driver-class\n" +
            "  jdbc-connect-url\n" +
            "  user\n" +
            "  password\n"
        );

        ps.println(
            "dbmd properties file properties:\n  " +
            "  date-mapping (DATES_AS_DRIVER_REPORTED | DATES_AS_TIMESTAMPS | DATES_AS_DATES)\n" +
            "  relations-owner (schema name | *any-owners*)\n" +
            "  exclude-relations-fqname-regex\n"
        );
    }

    public static void main(String[] args) throws Exception
    {
        if ( args.length == 1 && args[0].equals("-h") || args[0].equals("--help") )
        {
            printUsage(System.out);
            System.exit(0);
        }
        else if ( args.length < 2 )
        {
            printUsage(System.err);
            System.exit(1);
        }

        int argIx = 0;

        String jdbcPropsFilePath = args[argIx++];
        Optional<String> dbmdPropsFilePath = args.length == 3 ? Optional.of(args[argIx++]) : Optional.empty();
        String outputFilePath = args[argIx];

        Properties props = new Properties();

        try ( OutputStream os = outputStream(outputFilePath);
              InputStream propsIS = new FileInputStream(jdbcPropsFilePath) )
        {
            props.load(propsIS);

            String driverClassname = requireProperty(props, "jdbc-driver-class", "jdbc.driverClassName");
            String connStr = requireProperty(props, "jdbc-connect-url", "jdbc.url");
            String user = requireProperty(props, "user", "jdbc.username");
            String password = requireProperty(props, "password", "jdbc.password");

            Class.forName(driverClassname);

            try ( Connection conn = DriverManager.getConnection(connStr, user, password) )
            {
                dbmdPropsFilePath.ifPresent(dbmdPropsPath -> {
                    try {
                        if (!jdbcPropsFilePath.equals(dbmdPropsPath)) props.load(new FileInputStream(dbmdPropsPath));
                    } catch (IOException ioe) {
                        throw new RuntimeException(ioe);
                    }
                });

                Optional<String> dateMappingStr = getProperty(props, "date-mapping");
                DateMapping dateMapping = dateMappingStr.map(DateMapping::valueOf).orElse(DateMapping.DATES_AS_DRIVER_REPORTED);

                Optional<String> relsOwner = getProperty(props, "relations-owner").flatMap(o ->
                    o.equals("*any-owners*") ? Optional.empty() : Optional.of(o)
                );

                Optional<Pattern> excludeRelsPat =
                    getProperty(props, "exclude-relations-fqname-regex").map(Pattern::compile);

                DBMD dbmd =
                    new DatabaseMetaDataFetcher(dateMapping)
                    .fetchMetaData(
                        conn.getMetaData(),
                        relsOwner,
                        true,
                        true,
                        true,
                        excludeRelsPat
                    );

                ObjectMapper mapper = new ObjectMapper();
                mapper.registerModule(new Jdk8Module());
                mapper.enable(SerializationFeature.INDENT_OUTPUT);
                mapper.writeValue(os, dbmd);
            }
        }
    }
}
