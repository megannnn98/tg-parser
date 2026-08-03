package com.telegramcomments.app

import android.content.Intent
import android.net.http.SslError
import android.os.Bundle
import android.view.Menu
import android.view.MenuItem
import android.view.View
import android.webkit.SslErrorHandler
import android.webkit.WebResourceError
import android.webkit.WebResourceRequest
import android.webkit.WebResourceResponse
import android.webkit.WebView
import android.webkit.WebViewClient
import androidx.activity.OnBackPressedCallback
import androidx.appcompat.app.AppCompatActivity
import com.telegramcomments.app.BuildConfig
import com.telegramcomments.app.databinding.ActivityMainBinding

class MainActivity : AppCompatActivity() {

    private lateinit var binding: ActivityMainBinding
    private lateinit var settingsStore: SettingsStore
    private var lastLoadedUrl: String? = null

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        settingsStore = SettingsStore(SharedPreferencesKeyValueStore(this))
        binding = ActivityMainBinding.inflate(layoutInflater)
        setContentView(binding.root)
        setSupportActionBar(binding.toolbar)

        configureWebView()
        bindEmptyState()
        bindErrorOverlay()

        onBackPressedDispatcher.addCallback(this, object : OnBackPressedCallback(true) {
            override fun handleOnBackPressed() {
                when {
                    binding.errorOverlay.visibility == View.VISIBLE -> {
                        binding.errorOverlay.visibility = View.GONE
                    }
                    binding.webView.visibility == View.VISIBLE && binding.webView.canGoBack() -> {
                        binding.webView.goBack()
                    }
                    else -> {
                        isEnabled = false
                        onBackPressedDispatcher.onBackPressed()
                    }
                }
            }
        })
    }

    override fun onResume() {
        super.onResume()
        binding.webView.onResume()
        applyUrlState(forceReloadIfChanged = true)
    }

    override fun onPause() {
        super.onPause()
        binding.webView.onPause()
    }

    override fun onDestroy() {
        (binding.webView.parent as? android.view.ViewGroup)?.removeView(binding.webView)
        binding.webView.destroy()
        super.onDestroy()
    }

    override fun onCreateOptionsMenu(menu: Menu): Boolean {
        menuInflater.inflate(R.menu.main, menu)
        return true
    }

    override fun onOptionsItemSelected(item: MenuItem): Boolean {
        return when (item.itemId) {
            R.id.action_reload -> {
                binding.webView.reload()
                true
            }
            R.id.action_settings -> {
                startActivity(Intent(this, SettingsActivity::class.java))
                true
            }
            else -> super.onOptionsItemSelected(item)
        }
    }

    private fun configureWebView() {
        WebView.setWebContentsDebuggingEnabled(BuildConfig.DEBUG)
        with(binding.webView.settings) {
            javaScriptEnabled = true
            domStorageEnabled = true
            databaseEnabled = true
            allowFileAccess = false
            allowContentAccess = false
            mediaPlaybackRequiresUserGesture = true
        }
        binding.webView.webViewClient = object : WebViewClient() {
            override fun onReceivedError(
                view: WebView?,
                request: WebResourceRequest?,
                error: WebResourceError?,
            ) {
                if (request?.isForMainFrame == true) showError(error?.description?.toString())
            }

            override fun onReceivedHttpError(
                view: WebView?,
                request: WebResourceRequest?,
                errorResponse: WebResourceResponse?,
            ) {
                if (request?.isForMainFrame == true) {
                    val code = errorResponse?.statusCode?.toString() ?: "???"
                    showError(getString(R.string.err_load_failed, code))
                }
            }

            override fun onReceivedSslError(
                view: WebView?,
                handler: SslErrorHandler?,
                error: SslError?,
            ) {
                handler?.cancel()
                showError(getString(R.string.err_load_failed, error?.toString().orEmpty()))
            }
        }
    }

    private fun bindEmptyState() {
        binding.emptyStateButton.setOnClickListener {
            startActivity(Intent(this, SettingsActivity::class.java))
        }
    }

    private fun bindErrorOverlay() {
        binding.errorRetryButton.setOnClickListener {
            binding.errorOverlay.visibility = View.GONE
            binding.webView.reload()
        }
    }

    private fun applyUrlState(forceReloadIfChanged: Boolean) {
        val url = settingsStore.getUrl()
        when {
            url.isEmpty() -> {
                binding.emptyState.visibility = View.VISIBLE
                binding.webView.visibility = View.GONE
                binding.errorOverlay.visibility = View.GONE
            }
            forceReloadIfChanged && url != lastLoadedUrl -> {
                binding.emptyState.visibility = View.GONE
                binding.errorOverlay.visibility = View.GONE
                binding.webView.visibility = View.VISIBLE
                binding.webView.loadUrl(url)
                lastLoadedUrl = url
            }
            else -> {
                binding.emptyState.visibility = View.GONE
                binding.webView.visibility = View.VISIBLE
            }
        }
    }

    private fun showError(message: String?) {
        val text = message?.takeIf { it.isNotBlank() }
            ?: getString(R.string.err_load_failed, "")
        binding.errorText.text = text
        binding.errorOverlay.visibility = View.VISIBLE
    }
}
