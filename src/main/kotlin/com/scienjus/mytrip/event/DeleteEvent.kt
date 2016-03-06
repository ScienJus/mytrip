package com.scienjus.mytrip.event

/**
 * @author ScienJus
 * @date 16/3/5.
 */
data class DeleteEvent(val tableId: Long, val data: Map<String, Any?>) : LogEvent