package com.scienjus.mytrip

import com.github.shyiko.mysql.binlog.event.TableMapEventData
import com.scienjus.mytrip.config.Config
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.util.*

/**
 * @author ScienJus
 * @date 16/3/5.
 */

private val CACHE = HashMap<Long, TableInfo>()


fun getTableInfo(tableId: Long): TableInfo? {
    return CACHE[tableId]
}

fun createTableInfo(eventData: TableMapEventData) {
    if (!isInclude(eventData.database, eventData.table)) {
        return
    }
    val url = "jdbc:mysql://${Config.mysql.host}:${Config.mysql.port}/${eventData.database}"
    try {
        DriverManager.getConnection(url, Config.mysql.username, Config.mysql.password).use { conn ->
            conn.metaData.getColumns(null, null, eventData.table, null).use({ resultSet ->

                val primaryKey = getPrimaryKey(conn, eventData.table)

                val columnInfos = ArrayList<ColumnInfo>()

                while (resultSet.next()) {
                    val columnName = resultSet.getString("COLUMN_NAME")
                    val columnType = resultSet.getString("TYPE_NAME")
                    if (columnType.equals("enum", ignoreCase = true)) {
                        val enumValues = getEnumValues(conn, eventData.table, columnName)
                        columnInfos.add(ColumnInfo(columnName, columnType, enumValues))
                    } else {
                        columnInfos.add(ColumnInfo(columnName, columnType))
                    }
                }
                CACHE.put(eventData.tableId, TableInfo(eventData.database, eventData.table, primaryKey, columnInfos))
            })
        }
    } catch (e: SQLException) {
        throw RuntimeException("Get table information failed! table name : ${eventData.table}", e)
    }

}

private fun getPrimaryKey(conn: Connection, tableName: String): String {
    try {
        return conn.metaData.getPrimaryKeys(conn.catalog, null, tableName).use { resultSet ->
            if (resultSet.next()) {
                return@use resultSet.getString("COLUMN_NAME")
            }
            throw RuntimeException("Table must has a primary key! table name : $tableName")
        }
    } catch (e: SQLException) {
        throw RuntimeException("Get primary key failed! table name : $tableName", e)
    }

}

private fun getEnumValues(conn: Connection, tableName: String, columnName: String): List<String> {
    val sql = "show columns from $tableName like '$columnName'"
    try {
        return conn.prepareStatement(sql).use { statement ->
            return@use statement.executeQuery().use result@ { resultSet ->
                if (resultSet.next()) {
                    val enumValues = ArrayList<String>()
                    val enums = resultSet.getString("Type")
                    for (enumValue in enums.substring(5, enums.length - 1).split(",".toRegex()).dropLastWhile({ it.isEmpty() }).toTypedArray()) {
                        enumValues.add(enumValue.substring(1, enumValue.length - 1))
                    }
                    return@result enumValues
                }
                throw RuntimeException("Column $columnName from $tableName is not an enum")
            }
        }
    } catch (e: SQLException) {
        throw RuntimeException("Column $columnName from $tableName is not an enum", e)
    }

}