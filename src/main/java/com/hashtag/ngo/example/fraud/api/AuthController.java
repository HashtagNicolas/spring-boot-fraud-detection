package com.hashtag.ngo.example.fraud.api;

import com.hashtag.ngo.example.fraud.bean.JwtService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Émet des jetons JWT pour un unique compte de démonstration (demo/demo123) :
 * il n'y a pas de base d'utilisateurs derrière cet endpoint. Une vraie
 * application vérifierait les identifiants auprès d'un annuaire/une base
 * d'utilisateurs (avec mots de passe hashés), potentiellement via un
 * AuthenticationManager Spring Security dédié.
 */
@RestController
@RequestMapping("/auth")
public class AuthController {

    private static final String DEMO_USERNAME = "demo";
    private static final String DEMO_PASSWORD = "demo123";

    private final JwtService jwtService;

    public AuthController(JwtService jwtService) {
        this.jwtService = jwtService;
    }

    @PostMapping("/token")
    public ResponseEntity<TokenResponse> issueToken(@Valid @RequestBody AuthRequest request) {
        if (!DEMO_USERNAME.equals(request.username()) || !DEMO_PASSWORD.equals(request.password())) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }

        return ResponseEntity.ok(new TokenResponse(jwtService.generateToken(request.username())));
    }
}
