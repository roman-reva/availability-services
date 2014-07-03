package com.services.availability.common;

import java.io.*;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-06-23 17:50
 */
public class SerializationUtils {
    public static byte[] serialize(Object object) throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(os);
        oos.writeObject(object);
        return os.toByteArray();
    }

    public static Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        ObjectInputStream is = new ObjectInputStream(in);
        return is.readObject();
    }

}
