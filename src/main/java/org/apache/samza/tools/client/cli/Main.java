package org.apache.samza.tools.client.cli;
import  org.apache.samza.tools.client.impl.SamzaExecutor;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        // TODO: parse parameters here

        String[] arr = new String[123];
        Test(Arrays.asList(arr));

        CliShell shell = new CliShell();
        shell.open();

//        String testStmt = "select * from kafka.ProfileChangeStream";
//        SamzaExecutor cliExecutor = new SamzaExecutor();
//        cliExecutor.executeSql(Collections.singletonList(testStmt));
    }

    private  static void Test(List<String> a) {
    }

}
