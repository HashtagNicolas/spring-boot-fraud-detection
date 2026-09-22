package com.hashtag.ngo.example.fraud.bean;

/**
 * Génération et validation des jetons JWT utilisés pour authentifier les
 * appels à l'API REST.
 */
public interface JwtService {

    /**
     * @return un jeton JWT signé, dont le sujet est le nom d'utilisateur donné
     */
    String generateToken(String username);

    /**
     * @return le nom d'utilisateur (sujet) porté par un jeton valide
     * @throws IllegalArgumentException si le jeton n'est pas valide
     */
    String extractUsername(String token);

    /**
     * @return true si le jeton est correctement signé et n'a pas expiré
     */
    boolean isValid(String token);
}
