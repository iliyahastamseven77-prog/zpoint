plugins {
    id("com.android.application")
    id("org.jetbrains.kotlin.android")
}

android {
    namespace = "ir.zpoint.client"
    compileSdk = 34

    defaultConfig {
        applicationId = "ir.zpoint.client"
        minSdk = 26
        targetSdk = 34
        versionCode = 4
        versionName = "4.3-obsidian-hev"
        vectorDrawables { useSupportLibrary = true }
    }

    buildTypes {
        release {
            isMinifyEnabled = false
            isShrinkResources = false
        }
    }
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
    kotlinOptions {
        jvmTarget = "17"
    }
    buildFeatures {
        compose = true
    }
    composeOptions {
        // Compose compiler version paired with Kotlin 1.9.22 (official map)
        kotlinCompilerExtensionVersion = "1.5.8"
    }
    packaging {
        resources.excludes += "META-INF/*"
    }
}

dependencies {
    val composeBom = platform("androidx.compose:compose-bom:2024.02.01")
    implementation(composeBom)
    implementation("androidx.compose.ui:ui")
    implementation("androidx.compose.ui:ui-graphics")
    implementation("androidx.compose.material3:material3")
    // full icon set for the premium UI (adds ~40MB unpacked — explicitly
    // requested by the owner: quality over APK size)
    implementation("androidx.compose.material:material-icons-extended") {
        // avoid duplicate-class clash if icons-core sneaks in transitively
        // (extended contains every core icon)
        isTransitive = true
    }
    implementation("androidx.compose.animation:animation")
    implementation("androidx.activity:activity-compose:1.8.2")
    implementation("androidx.lifecycle:lifecycle-viewmodel-compose:2.7.0")
    implementation("androidx.lifecycle:lifecycle-runtime-compose:2.7.0")
    implementation("androidx.core:core-ktx:1.12.0")
    implementation("androidx.core:core-splashscreen:1.0.1")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-android:1.7.3")
}
