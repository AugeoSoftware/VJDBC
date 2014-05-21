package de.simplicit.vjdbc.servlet.kryo;

import java.net.URLConnection;

import de.simplicit.vjdbc.servlet.RequestModifier;

public class KryoRequestModifier implements RequestModifier {
    private final URLConnection _urlConnection;
    
    /**
     * Package visibility, doesn't make sense for other packages.
     * @param urlConnection Wrapped URLConnection
     */
    KryoRequestModifier(URLConnection urlConnection) {
        _urlConnection = urlConnection;
    }
    
    /* (non-Javadoc)
     * @see de.simplicit.vjdbc.servlet.RequestModifier#addRequestProperty(java.lang.String, java.lang.String)
     */
    public void addRequestHeader(String key, String value) {
        _urlConnection.addRequestProperty(key, value);
    }

}
