How to release:)
mvn versions:set -DnewVersion=0.6.5-BN-11-JDK11
mvn versions:commit
mvn clean install -DskipTests=true -P java-11
mvn deploy -DskipTests=true -DaltDeploymentRepository=soldr-backend::https://maven.pkg.github.com/mittmedia/soldr-backend -pl janusgraph-core,janusgraph-solr,janusgraph-driver,janusgraph-cql -am
