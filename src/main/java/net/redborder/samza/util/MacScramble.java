package net.redborder.samza.util;


import java.security.GeneralSecurityException;
import java.util.Arrays;

import org.bouncycastle.crypto.digests.SHA256Digest;
import org.bouncycastle.crypto.generators.PKCS5S2ParametersGenerator;
import org.bouncycastle.crypto.params.KeyParameter;

public class MacScramble {
    private static final byte[] MAC_PREFIX = "fdah7usad782345@".getBytes();

    private static final int PBKDF2_ITERATIONS = 10;
    private static final int PBKDF2_KEYSIZE = 48;
    private final byte[] spSalt;

    public MacScramble(final byte[] spSalt) {
        this.spSalt = Arrays.copyOf(spSalt, spSalt.length);
    }

    public byte[] scrambleMac(final byte[] mac) throws GeneralSecurityException {
        final PKCS5S2ParametersGenerator gen = new PKCS5S2ParametersGenerator(new SHA256Digest());

        final byte[] key = new byte[MAC_PREFIX.length + mac.length];
        System.arraycopy(MAC_PREFIX, 0, key, 0, MAC_PREFIX.length);
        for (int a=0;a<mac.length;a++) {
            key[MAC_PREFIX.length + a] = mac[a];
        }

        gen.init(key, this.spSalt, PBKDF2_ITERATIONS);
        return ((KeyParameter) gen.generateDerivedParameters(PBKDF2_KEYSIZE)).getKey();
    }
}

