package com.atguigu.flink.sql.catalog;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class CataLog {

    public static void main(String[] args) {

        TableEnvironment tableEnvironment = TableEnvironment.create(EnvironmentSettings.newInstance().build());

        String currentCatalog = tableEnvironment.getCurrentCatalog();
        System.out.println("currentCatalog = " + currentCatalog);

        String currentDatabase = tableEnvironment.getCurrentDatabase();
        System.out.println("currentDatabase = " + currentDatabase);

        String sql = "select 1";

        tableEnvironment.sqlQuery(sql).execute().print();


    }

}











