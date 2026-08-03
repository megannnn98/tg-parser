package com.telegramcomments.app

class SettingsStore(private val backend: KeyValueStore) {

    fun getUrl(): String = backend.getString(KEY_URL, "")

    fun setUrl(url: String) = backend.putString(KEY_URL, url)

    companion object {
        const val KEY_URL = "backend_url"
    }
}
