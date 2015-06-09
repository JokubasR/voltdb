package org.voltdb.regressionsuites;

import java.io.IOException;
import java.math.BigDecimal;

import org.voltdb.BackendTarget;
import org.voltdb.client.Client;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestDecimalRoundingSuite extends RegressionSuite {

    //
    // JUnit / RegressionSuite boilerplate
    //
    public TestDecimalRoundingSuite(String name) {
        super(name);
    }



    private void validateInsertStmt(String insertStmt, boolean exceptionExpected, BigDecimal... expectedValues) throws Exception {
        Client client = getClient();

        try {
            validateTableOfLongs(client, insertStmt, new long[][]{{1}});
            if (exceptionExpected) {
                fail("Exception expected in DECIMAL insert validation.");
            }
        } catch (Exception ex) {
            if (!exceptionExpected) {
                fail("Unexpected exception encountered in DECIMAL insert validation.");
            }

        }
        validateTableOfDecimal(client, "select * from decimaltable;", new BigDecimal[][] {expectedValues});
        validateTableOfLongs(client, "delete from decimaltable;", new long[][] {{1}});
    }

    public void testDecimalScaleInsertion() throws Exception {
       /*
        * Try some tests with no system properties.
        */
        // Sanity check.  See if we can insert a vanilla value.
        validateInsertStmt("insert into decimaltable values 0.9;",
                           false, /* no exception expected. */
                           new BigDecimal("0.900000000000"));
        // See if we can insert a value bigger then the fixed point
        // scale, and that we round up.
        validateInsertStmt("insert into decimaltable values 0.999999999999999;",
                           false, /* no exception expected. */
                           new BigDecimal("1.000000000000"));
        // Do the same as the last time, but make the last digit equal to 5.
        // This should round up.
        validateInsertStmt("insert into decimaltable values 0.999999999999500;",
                           false, /* no exception expected. */
                           new BigDecimal("1.000000000000"));
        // Do the same as the last time, but make the last digit equal to 4.
        // This should round down.
        validateInsertStmt("insert into decimaltable values 0.9999999999994000;",
                           false, /* no exception expected. */
                           new BigDecimal("0.999999999999"));
        validateInsertStmt("insert into decimaltable values null;",
                           false, /* no exception expected. */
                           (BigDecimal)null);

        /*
         * Disable rounding.
         */
        System.setProperty("BIGDECIMAL_ROUND", "false");
        String prop = System.getProperty("BIGDECIMAL_ROUND");
        // See if we can insert a value bigger then the fixed point
        // scale, and that we round up.
        validateInsertStmt("insert into decimaltable values 0.999999999999999;",
                           true, /* exception expected. */
                           new BigDecimal("1.000000000000"));
        // Do the same as the last time, but make the last digit equal to 5.
        // This should round up.
        validateInsertStmt("insert into decimaltable values 0.999999999999500;",
                           true, /* exception expected. */
                           new BigDecimal("1.000000000000"));
        // Do the same as the last time, but make the last digit equal to 4.
        // This should round down.
        validateInsertStmt("insert into decimaltable values 0.9999999999994000;",
                           true, /* exception expected. */
                           new BigDecimal("0.999999999999"));
        validateInsertStmt("insert into decimaltable values null;",
                           false, /* no exception expected. */
                           (BigDecimal)null);
    }


    static public junit.framework.Test suite() {

        LocalCluster config = null;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestDecimalRoundingSuite.class);
        VoltProjectBuilder project = new VoltProjectBuilder();
        final String literalSchema =
                "CREATE TABLE P1 ( id integer );" +
                "CREATE TABLE DECIMALTABLE ( " +
                "dec decimal" +
                ");" +
                ""
                ;
        try {
            project.addLiteralSchema(literalSchema);
        } catch (IOException e) {
            assertFalse(true);
        }
        boolean success;

        config = new LocalCluster("sqlinsert-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        config.setHasLocalServer(true);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        return builder;
    }
}
