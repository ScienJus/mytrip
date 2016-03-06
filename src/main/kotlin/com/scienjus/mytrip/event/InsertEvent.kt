package com.scienjus.mytrip.event

import java.io.Serializable

/**
 * @author ScienJus
 * @date 16/3/5.
 */
data class InsertEvent(val tableId: Long, val data: Map<String, Any?>) : LogEvent