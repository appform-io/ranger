package io.appform.ranger.discovery.bundle.id.nonce;

import lombok.experimental.UtilityClass;

@UtilityClass
public class NonceGenerators {
    
    private static final NonceGenerator randomNonceGenerator = new RandomNonceGenerator();
    
    public NonceGenerator randomNonceGenerator() {
        return randomNonceGenerator;
    }
}
