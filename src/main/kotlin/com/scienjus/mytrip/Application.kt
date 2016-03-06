package com.scienjus.mytrip

import com.github.shyiko.mysql.binlog.BinaryLogClient
import com.github.shyiko.mysql.binlog.event.*
import com.scienjus.mytrip.config.*
import com.scienjus.mytrip.event.DeleteEvent
import com.scienjus.mytrip.event.InsertEvent
import com.scienjus.mytrip.event.LogEvent
import com.scienjus.mytrip.event.UpdateEvent
import java.io.Serializable
import java.util.*
import java.util.concurrent.LinkedBlockingDeque

/**
 * @author ScienJus
 * @date 16/3/5.
 */
private val EVENT_QUEUE = LinkedBlockingDeque<LogEvent>(100)

fun main(args: Array<String>) {
    MySQLConfig.HOST = "localhost"
    MySQLConfig.PORT = 3306
    MySQLConfig.USERNAME = "root"
    MySQLConfig.PASSWORD = "4gmyMYSQL"

    Thread(Runnable {
        var event: LogEvent
        while (true) {
            event = EVENT_QUEUE.take();
            println(event)
        }
    }).start()

    val client = BinaryLogClient(MySQLConfig.HOST, MySQLConfig.PORT, MySQLConfig.USERNAME, MySQLConfig.PASSWORD)
    client.registerEventListener { event ->
        val eventData = event.getData<EventData>()
        when (eventData) {
            is TableMapEventData    -> createTableInfo(eventData.tableId, eventData.database, eventData.table)
            is WriteRowsEventData   -> createInsertEvent(eventData)
            is UpdateRowsEventData  -> createUpdateEvent(eventData)
            is DeleteRowsEventData  -> createDeleteEvent(eventData)
        }
    }
    client.connect()
}

fun createDeleteEvent(eventData: DeleteRowsEventData) {
    val includedColumns = eventData.includedColumns
    val tableInfo = getTableInfo(eventData.tableId)
    val columnInfos = tableInfo.columnInfos
    for (row in eventData.rows) {
        EVENT_QUEUE.push(DeleteEvent(eventData.tableId, rowToMap(columnInfos, includedColumns, row)))
    }
}

fun createUpdateEvent(eventData: UpdateRowsEventData) {
    val rows = eventData.rows;
    val tableInfo = getTableInfo(eventData.tableId)
    val columnInfos = tableInfo.columnInfos
    for (row in rows) {
        val before = rowToMap(columnInfos, eventData.includedColumnsBeforeUpdate, row.key)
        val data = rowToMap(columnInfos, eventData.includedColumns, row.value)
        EVENT_QUEUE.push(UpdateEvent(eventData.tableId, before, data))
    }
}

fun createInsertEvent(eventData: WriteRowsEventData) {
    val includedColumns = eventData.includedColumns
    val tableInfo = getTableInfo(eventData.tableId)
    val columnInfos = tableInfo.columnInfos
    for (row in eventData.rows) {
        EVENT_QUEUE.push(InsertEvent(eventData.tableId, rowToMap(columnInfos, includedColumns, row)))
    }
}

fun rowToMap(columnInfos: List<ColumnInfo>, includedColumns: BitSet, row: Array<Serializable>): Map<String, Any> {
    val data = HashMap<String, Any>()
    for (i in columnInfos.indices) {
        if (includedColumns.get(i)) {
            data.put(columnInfos[i].columnName, row[i])
        }
    }
    return data
}