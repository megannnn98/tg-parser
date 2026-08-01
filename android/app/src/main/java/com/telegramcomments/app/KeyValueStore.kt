package com.telegramcomments.app

interface KeyValueStore {
    fun getString(key: String, default: String): String
    fun putString(key: String, value: String)
}
