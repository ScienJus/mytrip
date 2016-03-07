package com.scienjus.mytrip.config

/**
 * @author XieEnlong
 * @date 2016/3/7.
 */
object Config {

    var mysql: MySQLConfig = MySQLConfig()
    var elasticsearch: ElasticsearchConfig = ElasticsearchConfig()
    val tables: List<String> = mutableListOf()
    val tableInfo: List<TableInfoConfig> = mutableListOf()

    override fun toString(): String {
        return "Config(mysql=$mysql, elasticsearch=$elasticsearch, tables=$tables, tableInfo=$tableInfo)"
    }
}