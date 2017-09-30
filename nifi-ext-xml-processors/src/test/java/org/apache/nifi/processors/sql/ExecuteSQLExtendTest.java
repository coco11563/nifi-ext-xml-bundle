package org.apache.nifi.processors.sql;

/**
 * Created by xjzhu@cnic.cn on 2017/9/21.
 */
public class ExecuteSQLExtendTest {

    /*@Test
    public void testXMLType() throws InitializationException {
        TestRunner testRunner = TestRunners.newTestRunner(new ExecuteSQLExtend());
        final AbstractControllerService cacheService = new DBCPConnectionPool();
        testRunner.addControllerService("Database Connection Pooling Service", cacheService);
//        testRunner.setProperty(cacheService, HiveConnectionPool.DATABASE_URL,"jdbc:hive2://10.0.82.168:2181,10.0.82.169:2181,10.0.82.170:2181,10.0.82.173:2181,10.0.82.172:2181,10.0.82.171:2181/default;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2");
        testRunner.setProperty(cacheService, DBCPConnectionPool.DATABASE_URL,"jdbc:oracle:thin:@(DESCRIPTION =(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=192.168.2.237)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=RACDB_STANDBY)))");
        testRunner.setProperty(cacheService, DBCPConnectionPool.DB_DRIVERNAME,"oracle.jdbc.OracleDriver");
        testRunner.setProperty(cacheService, DBCPConnectionPool.DB_DRIVER_LOCATION,"/opt/nifi-sql-bundle/lib/ojdbc7.jar");
        testRunner.setProperty(cacheService, DBCPConnectionPool.DB_USER,"dashuju");
        testRunner.setProperty(cacheService, DBCPConnectionPool.DB_PASSWORD,"DaShuju_732");
        testRunner.setProperty(cacheService, DBCPConnectionPool.MAX_WAIT_TIME,"500 millis" );
        testRunner.setProperty(cacheService, DBCPConnectionPool.MAX_TOTAL_CONNECTIONS,"48");
        testRunner.enableControllerService(cacheService);
        testRunner.setProperty(ExecuteSQLExtend.DBCP_SERVICE, "Database Connection Pooling Service");
        testRunner.setProperty(ExecuteSQLExtend.SQL_SELECT_QUERY,"SELECT * FROM (SELECT a.*, ROWNUM rnum FROM (SELECT * FROM egrant_isis.PROJECT ) a WHERE ROWNUM <= 400) WHERE rnum > 0");
        testRunner.run();
        
        final MockFlowFile flowFile =  testRunner.getFlowFilesForRelationship(ExecuteSQLExtend.REL_SUCCESS).get(0);
        flowFile.assertContentEquals("");
    }*/

}
