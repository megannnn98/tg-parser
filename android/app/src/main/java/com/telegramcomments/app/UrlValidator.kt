package com.telegramcomments.app

sealed class ValidationResult {
    object Empty : ValidationResult()
    data class Invalid(val reason: String) : ValidationResult()
    data class Valid(val normalized: String) : ValidationResult()
}

object UrlValidator {

    fun validate(input: String): ValidationResult {
        val raw = input.trim()
        if (raw.isEmpty()) return ValidationResult.Empty

        val withScheme = if (raw.contains("://")) raw else "http://$raw"

        val uri = try {
            java.net.URI(withScheme)
        } catch (e: java.net.URISyntaxException) {
            return ValidationResult.Invalid("не удалось разобрать URL")
        }

        val scheme = uri.scheme?.lowercase()
            ?: return ValidationResult.Invalid("не удалось разобрать URL")
        if (scheme != "http" && scheme != "https") {
            return ValidationResult.Invalid("поддерживаются только http и https")
        }

        val host = uri.host
        if (host.isNullOrBlank()) {
            return ValidationResult.Invalid("в URL отсутствует хост")
        }

        val normalized = buildString {
            append(scheme)
            append("://")
            append(host)
            if (uri.port != -1) {
                append(':')
                append(uri.port)
            }
            val path = uri.path.orEmpty()
            val trimmed = path.trimEnd('/')
            if (trimmed.isNotEmpty()) {
                append(trimmed)
            }
        }

        return ValidationResult.Valid(normalized)
    }
}
