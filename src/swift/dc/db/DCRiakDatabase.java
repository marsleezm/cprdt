package swift.dc.db;

import static sys.net.api.Networking.Networking;

import java.io.IOException;
import java.util.Properties;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.builders.RiakObjectBuilder;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.RiakResponse;
import com.basho.riak.client.raw.StoreMeta;
import com.basho.riak.client.raw.pbc.PBClientAdapter;
import com.basho.riak.pbc.RiakClient;

import swift.crdt.CRDTIdentifier;
import swift.dc.CRDTData;
import swift.dc.DCConstants;
import sys.net.impl.KryoSerializer;

public class DCRiakDatabase implements DCNodeDatabase {
    String url;
    int port;
    RawClient riak;

    @Override
    public void init(Properties props) {
        try {
            url = props.getProperty(DCConstants.RIAK_URL);
            port = Integer.parseInt(props.getProperty(DCConstants.RIAK_PORT));
            RiakClient pbcClient;
            pbcClient = new RiakClient(url);
            riak = new PBClientAdapter(pbcClient);
        } catch (IOException e) {
            throw new RuntimeException("Cannot contact Riak servers", e);
        }

    }

    @Override
    public synchronized CRDTData<?> read(CRDTIdentifier id) {
        try {
            RiakResponse response = riak.fetch( id.getTable(), id.getKey());
            DCConstants.DCLogger.info("RIAK.get " + id + ": response:" + response.hasValue());
            if( response.hasValue()) {
                if( response.hasSiblings()) {
                    IRiakObject[] obj = response.getRiakObjects();
                    CRDTData<?> data = (CRDTData<?>)Networking.serializer().readObject(obj[0].getValue());
                    data.getCrdt().init(data.getId(), data.getClock(), data.getPruneClock(), true);
                    for( int i = 1; i < obj.length; i++) {
                        CRDTData<?> t = (CRDTData<?>)Networking.serializer().readObject(obj[i].getValue());
                        t.getCrdt().init(t.getId(), t.getClock(), t.getPruneClock(), true);
                        data.mergeInto(t);
                    }
                    data.getCrdt().init(data.getId(), data.getClock(), data.getPruneClock(), true);
                    return data;
                } else {
                    IRiakObject[] obj = response.getRiakObjects();
                    byte[] arr = obj[0].getValue();
                    CRDTData<?> data = (CRDTData<?>)Networking.serializer().readObject(arr);
                    data.getCrdt().init(data.getId(), data.getClock(), data.getPruneClock(), true);
                    return data;
                }
            } else
                return null;
        } catch (IOException e) {
            DCConstants.DCLogger.throwing("DCRiakDatabase", "read", e);
            return null;
        }
    }

    @Override
    public synchronized boolean write(CRDTIdentifier id, CRDTData<?> data) {
        try {
            DCConstants.DCLogger.info("RIAK.store " + id + ": what:" + data.getId());
            
            byte[] arr = Networking.serializer().writeObject(data);
            IRiakObject riakObject = RiakObjectBuilder.newBuilder(id.getTable(), id.getKey()).withValue( arr).build();
            
            riak.store(riakObject);
            return true;
        } catch (IOException e) {
            DCConstants.DCLogger.throwing("DCRiakDatabase", "write", e);
            return false;
        }

    }

    @Override
    public boolean ramOnly() {
        return riak == null;
    }

    @Override
    public synchronized Object readSysData(String table, String key) {
        try{
        DCConstants.DCLogger.info("RIAK.SYSget " + table + ": response:" + key);
        RiakResponse response = riak.fetch(table, key);
        if( response.hasValue()) {
            if( response.hasSiblings()) {
                IRiakObject[] obj = response.getRiakObjects();
                Object[]objs = new Object[obj.length];
                for( int i = 0; i < obj.length; i++)
                    objs[i] = Networking.serializer().readObject(obj[0].getValue());
                return objs;
            } else {
                IRiakObject[] obj = response.getRiakObjects();
                return obj[0].getValue();
            }
        } else
            return null;
    } catch (IOException e) {
        DCConstants.DCLogger.throwing("DCRiakDatabase", "read", e);
        return null;
    }
    }

    @Override
    public synchronized boolean writeSysData(String table, String key, Object data) {
        try {
            DCConstants.DCLogger.info("RIAK.SYSstore " + table + ": what:" + key);
            
            byte[] arr = Networking.serializer().writeObject(data);
            IRiakObject riakObject = RiakObjectBuilder.newBuilder(table, key).withValue( arr).build();
            
            riak.store(riakObject);
            return true;
        } catch (IOException e) {
            DCConstants.DCLogger.throwing("DCRiakDatabase", "write", e);
            return false;
        }
    }

}
