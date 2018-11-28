package com.treutec

import java.util.Date

data class Person(
        val firstName: String,
        val lastName: String,
        val birthDate: Date,
        val city: String,
        val ipAddress: String
)