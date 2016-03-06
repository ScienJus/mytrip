package com.scienjus.mytrip

/**
 * @author ScienJus
 * @date 16/3/5.
 */
data class TableInfo(val databaseName: String, val tableName: String, val primaryKey: String, val columnInfos: List<ColumnInfo>)