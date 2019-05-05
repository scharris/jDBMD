package gov.fda.nctr.dbmd;

import java.util.*;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import com.fasterxml.jackson.annotation.JsonIgnore;


public class DBMD
{
    private Optional<String> schemaName;

    private List<RelMetaData> relMetaDatas;

    private List<ForeignKey> foreignKeys;

    private CaseSensitivity caseSensitivity;

    private String dbmsName;

    private String dbmsVersion;

    private int dbmsMajorVersion;

    private int dbmsMinorVersion;


    // derived data
    // Access these only via methods of the same name, which make sure these fields are initialized.
    @JsonIgnore()
    private Map<RelId, RelMetaData> relMDsByRelId;
    @JsonIgnore()
    private Map<RelId, List<ForeignKey>> fksByParentRelId;
    @JsonIgnore()
    private Map<RelId, List<ForeignKey>> fksByChildRelId;

    public enum ForeignKeyScope
    {
        REGISTERED_TABLES_ONLY,
        ALL_TABLES
    }

    public DBMD
        (
            Optional<String> schemaName,
            List<RelMetaData> relMetaDatas,
            List<ForeignKey> foreignKeys,
            CaseSensitivity caseSensitivity,
            String dbmsName,
            String dbmsVersion,
            int dbmsMajorVersion,
            int dbmsMinorVersion
        )
    {
        this.schemaName = schemaName;
        this.relMetaDatas = sortedMds(relMetaDatas);
        this.foreignKeys = sortedFks(foreignKeys);
        this.caseSensitivity = caseSensitivity;
        this.dbmsName = dbmsName;
        this.dbmsVersion = dbmsVersion;
        this.dbmsMajorVersion = dbmsMajorVersion;
        this.dbmsMinorVersion = dbmsMinorVersion;
    }

    protected DBMD() {}


    public Optional<String> getSchemaName() { return schemaName; }

    public List<RelMetaData> getRelationMetaDatas() { return relMetaDatas; }

    public List<ForeignKey> getForeignKeys() { return foreignKeys; }

    public CaseSensitivity getCaseSensitivity() { return caseSensitivity; }

    public String getDbmsName() { return dbmsName; }

    public String getDbmsVersion() { return dbmsVersion; }

    public int getDbmsMajorVersion() { return dbmsMajorVersion; }

    public int getDbmsMinorVersion() { return dbmsMinorVersion; }


    public Optional<RelMetaData> getRelationMetaData(RelId relId)
    {
        return Optional.ofNullable(relMDsByRelId().get(relId));
    }

    public Optional<RelMetaData> getRelationMetaData(Optional<String> schema, String relName)
    {
        return getRelationMetaData(makeRelId(schema, relName));
    }

    public List<String> getFieldNames(RelId relId, Optional<String> alias)
    {
        RelMetaData relMd = getRelationMetaData(relId).orElseThrow(() ->
            new IllegalArgumentException("Relation " + relId + " not found.")
        );

        return
            relMd.getFields().stream()
            .map(f -> dotQualify(alias, f.getName()))
            .collect(toList());
    }

    public List<String> getFieldNames(RelId relId)
    {
        return getFieldNames(relId, Optional.empty());
    }

    public List<String> getFieldNames(Optional<String> schema, String relName)
    {
        return getFieldNames(makeRelId(schema, relName));
    }

    public List<String> getFieldNames(Optional<String> schema, String relName, Optional<String> alias)
    {
        return getFieldNames(makeRelId(schema, relName), alias);
    }

    public List<String> getPrimaryKeyFieldNames(RelId relId, Optional<String> alias)
    {
        RelMetaData relMd = getRelationMetaData(relId).orElseThrow(() ->
            new IllegalArgumentException("Relation " + relId + " not found.")
        );

        return relMd.getPrimaryKeyFieldNames(alias);
    }

    public List<String> getPrimaryKeyFieldNames(RelId relId)
    {
        return getPrimaryKeyFieldNames(relId, Optional.empty());
    }

    public List<String> getPrimaryKeyFieldNames(Optional<String> schema, String relName)
    {
        return getPrimaryKeyFieldNames(makeRelId(schema, relName));
    }

    public List<String> getPrimaryKeyFieldNames(Optional<String> schema, String relName, Optional<String> alias)
    {
        return getPrimaryKeyFieldNames(makeRelId(schema, relName), alias);
    }

    public List<ForeignKey> getForeignKeysToParentsFrom(RelId relId)
    {
        return getForeignKeysFromTo(Optional.of(relId), Optional.empty());
    }

    public List<ForeignKey> getForeignKeysToParentsFrom(Optional<String> schema, String relName)
    {
        return getForeignKeysFromTo(Optional.of(makeRelId(schema, relName)), Optional.empty());
    }

    public List<ForeignKey> getForeignKeysFromChildrenTo(RelId relId)
    {
        return getForeignKeysFromTo(Optional.empty(), Optional.of(relId));
    }

    public List<ForeignKey> getForeignKeysFromChildrenTo(Optional<String> schema, String relName)
    {
        return getForeignKeysFromTo(Optional.empty(), Optional.of(makeRelId(schema, relName)));
    }

    public List<ForeignKey> getForeignKeysFromTo
        (
            String fromSchema,
            String fromRelName,
            String toSchema,
            String toRelName
        )
    {
        RelId fromRelId = makeRelId(Optional.of(fromSchema), fromRelName);
        RelId toRelId = makeRelId(Optional.of(toSchema), toRelName);

        return getForeignKeysFromTo(Optional.of(fromRelId), Optional.of(toRelId));
    }

    public List<ForeignKey> getForeignKeysFromTo
        (
            Optional<RelId> childRelId,
            Optional<RelId> parentRelId,
            ForeignKeyScope fkScope
        )
    {
        List<ForeignKey> res = new ArrayList<>();

        if ( childRelId.isEmpty() && parentRelId.isEmpty() )
        {
            res.addAll(foreignKeys);
        }
        else if ( childRelId.isPresent()  && parentRelId.isPresent() )
        {
            res.addAll(fksByChildRelId(childRelId.get()));
            res.retainAll(fksByParentRelId(parentRelId.get()));
        }
        else
            res.addAll(
                childRelId.isPresent() ?
                    fksByChildRelId(childRelId.get())
                    : fksByParentRelId(parentRelId.get())
            );

        if ( fkScope == ForeignKeyScope.REGISTERED_TABLES_ONLY )
        {
            return
                res.stream()
                .filter(fk ->
                    getRelationMetaData(fk.getSourceRelationId()).isPresent() &&
                    getRelationMetaData(fk.getTargetRelationId()).isPresent()
                )
                .collect(toList());
        }
        else
            return res;
    }

    public List<ForeignKey> getForeignKeysFromTo
        (
            Optional<RelId> childRelId,
            Optional<RelId> parentRelId
        )
    {
        return
            getForeignKeysFromTo(
                childRelId,
                parentRelId,
                ForeignKeyScope.REGISTERED_TABLES_ONLY
            );
    }

    /** Return a single foreign key between the passed tables, having the specified field names if specified,
     *  or Optional.empty() if not found. IllegalArgumentException is thrown if multiple foreign keys satisfy
     *  the requirements.
     */
    public Optional<ForeignKey> getForeignKeyFromTo
        (
            RelId fromRelId,
            RelId toRelId,
            Optional<Set<String>> fieldNames,
            ForeignKeyScope fkScope
        )
    {
        Optional<RelId> fromRel = Optional.of(fromRelId);
        Optional<RelId> toRel = Optional.of(toRelId);

        Optional<Set<String>> normdFkFieldNames = fieldNames.map(this::normalizeNames);

        ForeignKey soughtFk = null;

        for ( ForeignKey fk : getForeignKeysFromTo(fromRel, toRel, fkScope) )
        {
            if ( normdFkFieldNames.isEmpty() ||
                 fk.sourceFieldNamesSetEqualsNormalizedNamesSet(normdFkFieldNames.get()) )
            {
                if ( soughtFk != null ) // already found an fk satisfying requirements?
                    throw new IllegalArgumentException(
                    "Child table " + fromRelId + " has multiple foreign keys to parent table " + toRelId +
                    (fieldNames != null ? " with the same specified source fields."
                    : " and no foreign key fields were specified to disambiguate."));

                soughtFk = fk;
                // No breaking from the loop here, so case that multiple fk's satisfy requirements can be detected.
            }
        }

        return Optional.ofNullable(soughtFk);
    }

    /** Return the field names in the passed table involved in foreign keys (to parents). */
    public Set<String> getForeignKeyFieldNames
        (
            RelId relId,
            Optional<String> alias
        )
    {
        return
            getForeignKeysToParentsFrom(relId).stream()
            .flatMap(fk -> fk.getForeignKeyComponents().stream())
            .map(fkComp -> dotQualify(alias, fkComp.getForeignKeyFieldName()))
            .collect(toSet());
    }

    public Set<RelId> getMultiplyReferencingChildTablesForParent(RelId parentRelId)
    {
        Set<RelId> rels = new HashSet<>();
        Set<RelId> repeatedChildren = new HashSet<>();

        for ( ForeignKey fk : getForeignKeysFromChildrenTo(parentRelId) )
        {
            if ( !rels.add(fk.getSourceRelationId()) )
                repeatedChildren.add(fk.getSourceRelationId());
        }

        return repeatedChildren;
    }

    public Set<RelId> getMultiplyReferencedParentTablesForChild(RelId childRelId)
    {
        Set<RelId> rels = new HashSet<>();
        Set<RelId> repeatedParents = new HashSet<>();

        for ( ForeignKey fk : getForeignKeysToParentsFrom(childRelId) )
        {
            if ( !rels.add(fk.getTargetRelationId()) )
                repeatedParents.add(fk.getTargetRelationId());
        }

        return repeatedParents;
    }

    public Optional<ForeignKey> getForeignKeyHavingFieldSetAmong
        (
            Set<String> srcFieldNames,
            Collection<ForeignKey> fks
        )
    {
        Set<String> normdFieldNames = normalizeNames(srcFieldNames);

        return
            fks.stream()
            .filter(fk -> fk.sourceFieldNamesSetEqualsNormalizedNamesSet(normdFieldNames))
            .findAny();
    }

    /////////////////////////////////////////////////////////
    // Sorting for deterministic output

    private static List<RelMetaData> sortedMds(List<RelMetaData> relMds)
    {
        List<RelMetaData> rmds = new ArrayList<>(relMds);

        rmds.sort(Comparator.comparing(rmd -> rmd.getRelationId().getIdString()));

        return Collections.unmodifiableList(rmds);
    }

    /**
     * Return a new copy of the input list, with its foreign keys sorted by source and target relation names and source and target field names.
     */
    private static List<ForeignKey> sortedFks(List<ForeignKey> foreignKeys)
    {
        List<ForeignKey> fks = new ArrayList<>(foreignKeys);

        fks.sort((fk1, fk2) -> {
            int srcRelComp = fk1.getSourceRelationId().getIdString().compareTo(fk2.getSourceRelationId().getIdString());
            if (srcRelComp != 0)
                return srcRelComp;

            int tgtRelComp = fk1.getTargetRelationId().getIdString().compareTo(fk2.getTargetRelationId().getIdString());
            if (tgtRelComp != 0)
                return tgtRelComp;

            int srcFieldsComp = compareStringListsLexicographically(fk1.getSourceFieldNames(), fk2.getSourceFieldNames());

            if (srcFieldsComp != 0)
                return srcFieldsComp;
            else
                return compareStringListsLexicographically(fk1.getTargetFieldNames(), fk2.getTargetFieldNames());
        });

        return Collections.unmodifiableList(fks);
    }

    private static int compareStringListsLexicographically(List<String> strs1, List<String> strs2)
    {
        int commonCount = Math.min(strs1.size(), strs2.size());

        for (int i = 0; i < commonCount; ++i)
        {
            int comp = strs1.get(i).compareTo(strs2.get(i));
            if (comp != 0)
                return comp;
        }

        return strs1.size() < strs2.size() ? -1
        : strs1.size() > strs2.size() ? 1
        : 0;
    }

    // Sorting for deterministic output
    /////////////////////////////////////////////////////////


    /////////////////////////////////////////////////////////
    // Derived data accessor methods

    protected Map<RelId, RelMetaData> relMDsByRelId()
    {
        if ( relMDsByRelId == null )
            initDerivedData();

        return relMDsByRelId;
    }

    private List<ForeignKey> fksByParentRelId(RelId relId)
    {
        if ( fksByParentRelId == null )
            initDerivedData();

        List<ForeignKey> fks = fksByParentRelId.get(relId);
        if ( fks != null )
            return fks;
        else
            return Collections.emptyList();
    }

    private List<ForeignKey> fksByChildRelId(RelId relId)
    {
        if ( fksByChildRelId == null )
            initDerivedData();

        List<ForeignKey> fks = fksByChildRelId.get(relId);
        if ( fks != null )
            return fks;
        else
            return Collections.emptyList();
    }

    protected void initDerivedData()
    {
        relMDsByRelId = new HashMap<>();
        fksByParentRelId = new HashMap<>();
        fksByChildRelId = new HashMap<>();

        for ( RelMetaData relMd : relMetaDatas )
            relMDsByRelId.put(relMd.getRelationId(), relMd);

        for (ForeignKey fk : foreignKeys)
        {
            RelId srcRelId = fk.getSourceRelationId();
            RelId tgtRelId = fk.getTargetRelationId();

            List<ForeignKey> fksFromChild = fksByChildRelId.computeIfAbsent(srcRelId, k -> new ArrayList<>());
            fksFromChild.add(fk);

            List<ForeignKey> fksToParent = fksByParentRelId.computeIfAbsent(tgtRelId, k -> new ArrayList<>());
            fksToParent.add(fk);
        }
    }

    // Derived data accessor methods
    /////////////////////////////////////////////////////////

    ////////////////////////////////////////////////////////
    // Database object name manipulations

    // Normalize a database object name.
    private String normalizeName(String id)
    {
        if ( id.startsWith("\"") && id.endsWith("\"") )
            return id;
        else if ( caseSensitivity == CaseSensitivity.INSENSITIVE_STORED_LOWER )
            return id.toLowerCase();
        else if ( caseSensitivity == CaseSensitivity.INSENSITIVE_STORED_UPPER )
            return id.toUpperCase();
        else
            return id;
    }

    private Set<String> normalizeNames(Set<String> names)
    {
        return names.stream().map(this::normalizeName).collect(toSet());
    }


    public <E> Map<String, E> normalizeNameKeys(Map<String, E> mapWithIdentifierKeys)
    {
        Map<String, E> res = new HashMap<>();

        for (Map.Entry<String, E> entry : mapWithIdentifierKeys.entrySet())
        {
            res.put(normalizeName(entry.getKey()), entry.getValue());
        }

        return res;
    }

    private static String dotQualify(Optional<String> alias, String name)
    {
        return alias.map(a -> a + "." + name).orElse(name);
    }


    public RelId makeRelId(Optional<String> catalog, Optional<String> schema, String relName)
    {
        return new RelId(catalog.map(this::normalizeName), schema.map(this::normalizeName), normalizeName(relName));
    }

    public RelId makeRelId(Optional<String> schema, String relName)
    {
        return new RelId(Optional.empty(), schema.map(this::normalizeName), normalizeName(relName));
    }

    public RelId makeRelId(String possiblySchemaQualifiedRelName)
    {
        Optional<String> schema;
        String relName;

        int dotix = possiblySchemaQualifiedRelName.indexOf('.');

        if ( dotix == -1 )
        {
            schema = getSchemaName();
            relName = possiblySchemaQualifiedRelName;
        }
        else
        {
            schema = Optional.of(possiblySchemaQualifiedRelName.substring(0, dotix));
            relName = possiblySchemaQualifiedRelName.substring(dotix + 1);
        }

        return new RelId(Optional.empty(), schema.map(this::normalizeName), normalizeName(relName));
    }

    // Database object name manipulations
    ////////////////////////////////////////////////////////
}
