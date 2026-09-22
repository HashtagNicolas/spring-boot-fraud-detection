package com.hashtag.ngo.example.fraud.api;

import jakarta.validation.constraints.NotBlank;

public record AuthRequest(
        @NotBlank
        String username,

        @NotBlank
        String password
) {
}
