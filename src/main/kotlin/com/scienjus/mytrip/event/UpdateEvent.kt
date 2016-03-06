package com.scienjus.mytrip.event

/**
 * @author ScienJus
 * @date 16/3/5.
 */
data class UpdateEvent(val tableId: Long, val before: Map<String, Any?>, val data: Map<String, Any?>) : LogEvent