package com.atguigu.flink.sql.catalog;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import java.util.Arrays;

public class IHiveCataLog {

    public static void main(String[] args) {

        TableEnvironment tableEnvironment = TableEnvironment.create(EnvironmentSettings.newInstance().build());

        //创建HiveCatalog
        HiveCatalog hiveCatalog = new HiveCatalog(
                "hive",
                "default",
                "conf"
        );
        //注册Catalog
        tableEnvironment.registerCatalog("hive",hiveCatalog);
        //使用catalog
        tableEnvironment.useCatalog("hive");

        String currentCatalog = tableEnvironment.getCurrentCatalog();
        System.out.println("currentCatalog = " + currentCatalog);

        String currentDatabase = tableEnvironment.getCurrentDatabase();
        System.out.println("currentDatabase = " + currentDatabase);

        System.out.println(Arrays.toString(tableEnvironment.listTables()));



    }

}
