// VJDBC - Virtual JDBC
// Written by Michael Link
// Website: http://vjdbc.sourceforge.net

package de.simplicit.vjdbc.serial;

import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

/**
 * Initially this class is intended for transporting objects 
 * and compressing the serialized object "on the fly".
 * However compression cause more problems than solves,
 * TODO fix compression.
 * @author semenov
 *
 */
@Deprecated
public class SerializableTransport implements Externalizable {
    static final long serialVersionUID = -5634734498572640609L;

    private boolean _isCompressed;
    private Object _transportee;
    
//    private transient Object _original;

    private transient ByteArrayOutputStream _baos = null;
    private transient int _compressionMode;
    
    public SerializableTransport() {
    }
    
    public SerializableTransport(Object transportee, int compressionMode, long minimumSize) {
        _compressionMode = compressionMode;
    	deflate(transportee, compressionMode, minimumSize);
    }

    public SerializableTransport(Object transportee) {
        this(transportee, Deflater.BEST_SPEED, 2000);
    }

    public Object getTransportee() throws IOException, ClassNotFoundException {
//        if(_original == null) {
////            if(_isCompressed) {
////                inflate();
////            } else {
//                _original = _transportee;
////            }
//        }
//
//        return _original;
    	return _transportee;
    }

    private void deflate(Object crs, int compressionMode, long minimumSize) {
    	if(compressionMode != Deflater.NO_COMPRESSION) {
            try {
                _baos = serializeObject(crs);
                _isCompressed = _baos.size() >= minimumSize;
                _transportee = crs;
//                if(_baos.size() >= minimumSize) {
////                    _transportee = Zipper.zip(serializedObject, compressionMode);
//                    _isCompressed = true;
//                } else {
//                    _transportee = crs;
//                    _isCompressed = false;
//                }
            } catch(IOException e) {
                _transportee = crs;
                _isCompressed = false;
            }
        } else {
            _transportee = crs;
            _isCompressed = false;
        }
    }

//    private void inflate() throws IOException, ClassNotFoundException {
//        byte[] unzipped = Zipper.unzip((byte[])_transportee);
//        _original = deserializeObject(unzipped);
//    }
    
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        _isCompressed = in.readBoolean();
        if (in instanceof InputStream){
        	ObjectInputStream ois;
    		if (_isCompressed){
    			ois = new ObjectInputStream(new InflaterInputStream((InputStream) in));
    		} else {
    			ois = new ObjectInputStream((InputStream) in);
    		}
    		_transportee = ois.readObject();
        } else {
    		throw new UnsupportedOperationException("deserializing from "+in.getClass()+" is not supported");
    	}
    }

	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeBoolean(_isCompressed);
		if (out instanceof OutputStream) {
			if (_isCompressed) {
				OutputStream os = (OutputStream) out;
				Deflater deflater = new Deflater(_compressionMode);
				DeflaterOutputStream dos = new DeflaterOutputStream(os, deflater);
				_baos.writeTo(dos);
				dos.flush();
				dos.finish();
			} else if (_baos != null) {
				_baos.writeTo((OutputStream) out);
			} else {
				ObjectOutputStream os = new ObjectOutputStream((OutputStream) out);
				os.writeObject(_transportee);
			}
		} else {
			throw new UnsupportedOperationException("deserializing to " + out.getClass() + " is not supported");
		}
	}

    private static ByteArrayOutputStream serializeObject(Object obj) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(obj);
        oos.close();
        return baos;
    }

//    private static Object deserializeObject(byte[] b) throws ClassNotFoundException, IOException {
//        ByteArrayInputStream bais = new ByteArrayInputStream(b);
//        ObjectInputStream ois = new ObjectInputStream(bais);
//        return ois.readObject();
//    }
}
