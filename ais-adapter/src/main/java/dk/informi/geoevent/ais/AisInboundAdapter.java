package dk.informi.geoevent.ais;

import com.esri.ges.adapter.AdapterDefinition;
import com.esri.ges.adapter.InboundAdapterBase;
import com.esri.ges.core.component.ComponentException;
import com.esri.ges.core.geoevent.GeoEvent;
import dk.dma.ais.reader.AisReader;
import java.nio.ByteBuffer;
import dk.dma.ais.reader.AisReaders;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
public class AisInboundAdapter extends InboundAdapterBase {
     
    public AisInboundAdapter(AdapterDefinition definition) throws ComponentException {
        super(definition);
    }
            
    
    @Override
    protected GeoEvent adapt(ByteBuffer bb, String string) {
        InputStream stream = new ByteArrayInputStream(bb.array());
        AisReader reader = AisReaders.createReaderFromInputStream(stream);
        
        
        
        
        return null;
        
    }

}
