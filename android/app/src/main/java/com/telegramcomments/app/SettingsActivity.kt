package com.telegramcomments.app

import android.os.Bundle
import androidx.appcompat.app.AppCompatActivity
import com.telegramcomments.app.databinding.ActivitySettingsBinding

class SettingsActivity : AppCompatActivity() {

    private lateinit var binding: ActivitySettingsBinding
    private lateinit var settingsStore: SettingsStore

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        settingsStore = SettingsStore(SharedPreferencesKeyValueStore(this))
        binding = ActivitySettingsBinding.inflate(layoutInflater)
        setContentView(binding.root)
        setSupportActionBar(binding.toolbar)
        supportActionBar?.setDisplayHomeAsUpEnabled(true)
        binding.toolbar.setNavigationOnClickListener { finish() }

        binding.urlInput.setText(settingsStore.getUrl())

        binding.saveButton.setOnClickListener { onSavePressed() }
    }

    private fun onSavePressed() {
        val input = binding.urlInput.text?.toString().orEmpty()
        when (val result = UrlValidator.validate(input)) {
            ValidationResult.Empty -> {
                binding.urlInputLayout.error = getString(R.string.err_empty_url)
            }
            is ValidationResult.Invalid -> {
                binding.urlInputLayout.error = result.reason
            }
            is ValidationResult.Valid -> {
                binding.urlInputLayout.error = null
                settingsStore.setUrl(result.normalized)
                binding.urlInput.setText(result.normalized)
                finish()
            }
        }
    }
}
