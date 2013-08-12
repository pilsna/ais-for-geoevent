package dk.informi.geoevent.ais;

import com.esri.ges.adapter.AdapterDefinition;
import com.esri.ges.adapter.InboundAdapterBase;
import com.esri.ges.core.component.ComponentException;
import com.esri.ges.core.geoevent.FieldException;
import com.esri.ges.core.geoevent.GeoEvent;
import com.esri.ges.core.geoevent.GeoEventDefinition;
import dk.dma.ais.reader.AisReader;
import java.nio.ByteBuffer;
import dk.dma.ais.reader.AisReaders;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import dk.dma.ais.message.AisMessage;
import dk.dma.enav.model.geometry.Position;
import java.nio.BufferUnderflowException;
import java.util.logging.Level;
import java.util.logging.Logger;
import dk.dma.enav.util.function.Consumer;

public class AisInboundAdapter extends InboundAdapterBase {

    public AisInboundAdapter(AdapterDefinition definition) throws ComponentException {
        super(definition);
    }

    @Override
    protected GeoEvent adapt(ByteBuffer buffer, String string) {
        InputStream stream = new ByteArrayInputStream(buffer.array());
        AisReader reader = AisReaders.createReaderFromInputStream(stream);
        reader.registerHandler(new Consumer<AisMessage>() {
            @Override
            public void accept(AisMessage aisMessage) {
                System.out.println("message id: " + aisMessage.getMsgId());
                Position position = aisMessage.getValidPosition();
                
            }
        });
        reader.start();
        try {
            reader.join();
        } catch (InterruptedException ex) {
            Logger.getLogger(AisInboundAdapter.class.getName()).log(Level.SEVERE, null, ex);
        } catch (BufferUnderflowException ex) {
            buffer.reset();
            return null;
        } /*catch (FieldException e) {
         e.printStackTrace();
         return null;
         }*/



        return null;

    }
}
