package com.amane.tools;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5Tools {

    private static final String salt = "9ice0touch8";

    public static String md5(String text) {
        MessageDigest digest = null;
        String realText = text.concat(salt);
        try {
            digest = MessageDigest.getInstance("md5");
            byte[] result = digest.digest(realText.getBytes());
            StringBuilder sb = new StringBuilder();
            for (byte b : result) {
                int number = b & 0xff;
                String hex = Integer.toHexString(number);
                if (hex.length() == 1) {
                    sb.append("0").append(hex);
                } else {
                    sb.append(hex);
                }
            }
            return sb.toString().substring(8, 24).toUpperCase();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return null;
    }
}
