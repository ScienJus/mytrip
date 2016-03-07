package com.scienjus.mytrip

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.JSONObject
import com.github.shyiko.mysql.binlog.BinaryLogClient
import com.github.shyiko.mysql.binlog.event.*
import com.scienjus.mytrip.config.Config
import com.scienjus.mytrip.event.DeleteEvent
import com.scienjus.mytrip.event.InsertEvent
import com.scienjus.mytrip.event.LogEvent
import com.scienjus.mytrip.event.UpdateEvent
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.transport.InetSocketTransportAddress
import java.io.Serializable
import java.util.*
import java.util.concurrent.LinkedBlockingDeque

/**
 * @author ScienJus
 * @date 16/3/5.
 */
private val EVENT_QUEUE = LinkedBlockingDeque<LogEvent>(100)

fun main(args: Array<String>) {

    initConfig();

    Thread(Runnable {
        val client = TransportClient()
                .addTransportAddress(InetSocketTransportAddress(Config.elasticsearch.host, Config.elasticsearch.port))
        var event: LogEvent
        while (true) {
            event = EVENT_QUEUE.take();
            println(event)
            when (event) {
                is InsertEvent  -> indexData(client, event)
                is UpdateEvent  -> updateData(client, event)
                is DeleteEvent  -> deleteData(client, event)
            }
        }
    }).start()

    val client = BinaryLogClient(Config.mysql.host, Config.mysql.port, Config.mysql.username, Config.mysql.password)
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

fun <T> JSONObject.getArray(key: String, clazz: Class<T>): List<T> {
    return JSON.parseArray(this.getJSONArray(key).toJSONString(), clazz)
}

fun <T> JSONObject.get(key: String, clazz: Class<T>): T {
    return JSON.parseObject(this.getJSONObject(key).toJSONString(), clazz)
}

fun initConfig() {
    val configFile = TableInfo::class.java.classLoader.getResourceAsStream("config-example.json")
    JSON.parseObject(configFile.reader().readText(), Config::class.java)
}

fun deleteData(client: TransportClient, event: DeleteEvent) {
    val tableInfo = event.tableInfo
    if (Config.tables.isNotEmpty() && !Config.tables.contains(tableInfo.databaseName + "." + tableInfo.tableName)) {
        return
    }
    val indexData = configIndexData(tableInfo, event.data)
    val primaryKeyValue = event.data.get(tableInfo.primaryKey)
    client.prepareDelete(indexData.indexName, indexData.typeName, primaryKeyValue!!.toString())
            .execute()
            .actionGet();
}

fun indexData(client: TransportClient, event: InsertEvent) {
    val tableInfo = event.tableInfo
    if (Config.tables.isNotEmpty() && !Config.tables.contains(tableInfo.databaseName + "." + tableInfo.tableName)) {
        return
    }
    val indexData = configIndexData(tableInfo, event.data)
    val primaryKeyValue = event.data.get(tableInfo.primaryKey)
    client.prepareIndex(indexData.indexName, indexData.typeName, primaryKeyValue!!.toString())
            .setSource(indexData.data)
            .execute()
            .actionGet();
}

fun configIndexData(tableInfo: TableInfo, data: Map<String, Any?>): IndexData {
    val config = Config.tableInfo.firstOrNull {
        it.tableName == tableInfo.databaseName + "." + tableInfo.tableName
    }
    val indexName = config?.aliasName?.split("\\.")?.get(0) ?: tableInfo.databaseName
    val typeName = config?.aliasName?.split("\\.")?.get(1) ?: tableInfo.tableName
    var indexData = data
    if (config != null) {
        val mutableMap: MutableMap<String, Any?> = mutableMapOf()
        if (config.columns.isNotEmpty()) {
            mutableMap.putAll(data.filterKeys {
                config.columns.contains(it)
            })
        } else {
            mutableMap.putAll(data)
        }
        config.columnInfo.forEach {
            if (data.containsKey(it.columnName)) {
                val value = mutableMap.remove(it.columnName)
                mutableMap.put(it.aliasName, value)
            }
        }
        indexData = mutableMap
    }
    return IndexData(indexName, typeName, indexData)
}

fun updateData(client: TransportClient, event: UpdateEvent) {
    val tableInfo = event.tableInfo
    if (Config.tables.isNotEmpty() && !Config.tables.contains(tableInfo.databaseName + "." + tableInfo.tableName)) {
        return
    }
    val indexData = configIndexData(tableInfo, event.data)
    val primaryKeyValue = event.data.get(tableInfo.primaryKey)
    client.prepareUpdate(indexData.indexName, indexData.typeName, primaryKeyValue!!.toString())
            .setDoc(indexData.data)
            .get();
}

fun createDeleteEvent(eventData: DeleteRowsEventData) {
    val includedColumns = eventData.includedColumns
    val tableInfo = getTableInfo(eventData.tableId)
    val columnInfos = tableInfo.columnInfos
    for (row in eventData.rows) {
        EVENT_QUEUE.push(DeleteEvent(tableInfo, rowToMap(columnInfos, includedColumns, row)))
    }
}

fun createUpdateEvent(eventData: UpdateRowsEventData) {
    val rows = eventData.rows;
    val tableInfo = getTableInfo(eventData.tableId)
    val columnInfos = tableInfo.columnInfos
    for (row in rows) {
        val before = rowToMap(columnInfos, eventData.includedColumnsBeforeUpdate, row.key)
        val data = rowToMap(columnInfos, eventData.includedColumns, row.value)
        EVENT_QUEUE.push(UpdateEvent(tableInfo, before, data))
    }
}

fun createInsertEvent(eventData: WriteRowsEventData) {
    val includedColumns = eventData.includedColumns
    val tableInfo = getTableInfo(eventData.tableId)
    val columnInfos = tableInfo.columnInfos
    for (row in eventData.rows) {
        EVENT_QUEUE.push(InsertEvent(tableInfo, rowToMap(columnInfos, includedColumns, row)))
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