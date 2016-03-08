package com.scienjus.mytrip.config

/**
 * @author ScienJus
 * @date 2016/3/7.
 */
data class TableInfoConfig(
        var tableName: String = "",
        var aliasName: String? = null,
        val columns: List<String> = mutableListOf(),
        val columnInfo: List<ColumnInfoConfig> = mutableListOf()
)