// Top-level build file for Zpoint Android client.
plugins {
    id("com.android.application") version "8.1.4" apply false
    id("org.jetbrains.kotlin.android") version "1.9.22" apply false
    // Kotlin 2.x compose plugin is NOT used — with Kotlin 1.9.22 the
    // Compose compiler ships WITH the Kotlin distribution and is wired
    // per-module via composeOptions.kotlinCompilerExtensionVersion.
}
