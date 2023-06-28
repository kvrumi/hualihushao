package com.atguigu.flink.sql.catalog;

import org.apache.flink.connector.jdbc.catalog.JdbcCatalog;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class IJdbcCataLog {

    public static void main(String[] args) {

        TableEnvironment tableEnvironment = TableEnvironment.create(EnvironmentSettings.newInstance().build());

        JdbcCatalog jdbcCatalog = new JdbcCatalog(
                IJdbcCataLog.class.getClassLoader(),
                "mysql",
                "test",
                "root",
                "000000",
                "jdbc:mysql://hadoop102:3306"
        );
        //注册Catalog
        tableEnvironment.registerCatalog("mysql",jdbcCatalog);
        //使用catalog
        tableEnvironment.useCatalog("mysql");



    }

}
