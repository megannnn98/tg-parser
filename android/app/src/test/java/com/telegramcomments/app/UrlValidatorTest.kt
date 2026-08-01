package com.telegramcomments.app

import org.junit.Assert.assertEquals
import org.junit.Test

class UrlValidatorTest {

    @Test
    fun `empty input is rejected as Empty`() {
        assertEquals(ValidationResult.Empty, UrlValidator.validate(""))
    }

    @Test
    fun `blank input is rejected as Empty`() {
        assertEquals(ValidationResult.Empty, UrlValidator.validate("   \t\n"))
    }

    @Test
    fun `valid http URL with port is accepted and trimmed`() {
        assertEquals(
            ValidationResult.Valid("http://192.168.1.5:8000"),
            UrlValidator.validate("  http://192.168.1.5:8000  "),
        )
    }

    @Test
    fun `valid https URL is accepted`() {
        assertEquals(
            ValidationResult.Valid("https://example.com"),
            UrlValidator.validate("https://example.com"),
        )
    }

    @Test
    fun `trailing slash is stripped`() {
        assertEquals(
            ValidationResult.Valid("https://example.com"),
            UrlValidator.validate("https://example.com/"),
        )
    }

    @Test
    fun `path is preserved`() {
        assertEquals(
            ValidationResult.Valid("https://example.com/foo"),
            UrlValidator.validate("https://example.com/foo"),
        )
    }

    @Test
    fun `missing scheme defaults to http`() {
        assertEquals(
            ValidationResult.Valid("http://localhost:8000"),
            UrlValidator.validate("localhost:8000"),
        )
    }

    @Test
    fun `bare hostname defaults to http`() {
        assertEquals(
            ValidationResult.Valid("http://example.com"),
            UrlValidator.validate("example.com"),
        )
    }

    @Test
    fun `ftp scheme is rejected`() {
        val result = UrlValidator.validate("ftp://example.com")
        assert(result is ValidationResult.Invalid)
    }

    @Test
    fun `scheme without host is rejected`() {
        val result = UrlValidator.validate("http://")
        assert(result is ValidationResult.Invalid)
    }

    @Test
    fun `garbage is rejected`() {
        val result = UrlValidator.validate("ht!tp://bad")
        assert(result is ValidationResult.Invalid)
    }

    @Test
    fun `Invalid result carries a non-blank reason`() {
        val result = UrlValidator.validate("ht!tp://bad")
        assert(result is ValidationResult.Invalid) { "expected Invalid" }
        val reason = (result as ValidationResult.Invalid).reason
        assert(reason.isNotBlank()) { "reason must not be blank" }
    }

    @Test
    fun `Valid result normalized url starts with http`() {
        val result = UrlValidator.validate("https://example.com")
        assert(result is ValidationResult.Valid)
        val normalized = (result as ValidationResult.Valid).normalized
        assert(normalized.startsWith("http")) { "normalized must start with http(s)://" }
    }
}
