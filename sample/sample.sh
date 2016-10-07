#!/bin/sh
dbmdjar=$(ls ../target/dbmd-*.jar | egrep -v sources)

java -cp $dbmdjar:ojdbc6.jar gov.fda.nctr.dbmd.DatabaseMetaDataFetcher sample.props sample.props sample.xml
