package com.telegramcomments.app

import org.junit.Assert.assertEquals
import org.junit.Test

class SettingsStoreTest {

    private class FakeKeyValueStore : KeyValueStore {
        private val map = mutableMapOf<String, String>()

        override fun getString(key: String, default: String): String =
            map[key] ?: default

        override fun putString(key: String, value: String) {
            map[key] = value
        }

        fun stored(key: String): String? = map[key]
    }

    @Test
    fun `getUrl returns empty string by default`() {
        val store = SettingsStore(FakeKeyValueStore())
        assertEquals("", store.getUrl())
    }

    @Test
    fun `setUrl persists under the expected key`() {
        val backend = FakeKeyValueStore()
        SettingsStore(backend).setUrl("https://example.com")
        assertEquals("https://example.com", backend.stored(SettingsStore.KEY_URL))
    }

    @Test
    fun `getUrl returns the value written by setUrl`() {
        val store = SettingsStore(FakeKeyValueStore())
        store.setUrl("http://192.168.1.5:8000")
        assertEquals("http://192.168.1.5:8000", store.getUrl())
    }

    @Test
    fun `setUrl with empty string clears stored value`() {
        val backend = FakeKeyValueStore()
        val store = SettingsStore(backend)
        store.setUrl("https://example.com")
        store.setUrl("")
        assertEquals("", store.getUrl())
        assertEquals("", backend.stored(SettingsStore.KEY_URL))
    }

    @Test
    fun `setUrl overwrites previous value`() {
        val store = SettingsStore(FakeKeyValueStore())
        store.setUrl("https://a.example")
        store.setUrl("https://b.example")
        assertEquals("https://b.example", store.getUrl())
    }
}
