#!/bin/sh
SCRIPTDIR=$(dirname "$0")

JAR=$(ls $SCRIPTDIR/../target/dbmd-*.jar | egrep -v sources)

java -cp $SCRIPTDIR/../target/dbmd.jar:postgresql-42.2.8.jar \
    gov.fda.nctr.dbmd.DatabaseMetadataFetcher sample.props sample.props -
