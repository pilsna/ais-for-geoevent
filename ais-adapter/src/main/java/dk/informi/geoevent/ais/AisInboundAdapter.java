package dk.informi.geoevent.ais;

import com.esri.ges.adapter.AdapterDefinition;
import com.esri.ges.adapter.InboundAdapterBase;
import com.esri.ges.core.component.ComponentException;
import com.esri.ges.core.geoevent.FieldException;
import com.esri.ges.core.geoevent.GeoEvent;
import com.esri.ges.messaging.MessagingException;
import dk.dma.ais.reader.AisReader;
import java.nio.ByteBuffer;
import dk.dma.ais.reader.AisReaders;
import java.io.ByteArrayInputStream;
import dk.dma.ais.message.AisMessage;
import dk.dma.ais.message.AisMessage21;
import dk.dma.ais.message.AisPositionMessage;
import dk.dma.ais.message.AisStaticCommon;
import dk.dma.ais.message.IVesselPositionMessage;
import dk.dma.ais.proprietary.IProprietarySourceTag;
import dk.dma.enav.model.geometry.Position;
import java.nio.BufferUnderflowException;
import dk.dma.enav.util.function.Consumer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class AisInboundAdapter extends InboundAdapterBase {

    private static final Log log = LogFactory.getLog(AisInboundAdapterService.class);

    public AisInboundAdapter(AdapterDefinition definition) throws ComponentException {
        super(definition);
        log.info("Created AisInboundAdapter.");
    }

    @Override
    public void receive(ByteBuffer buffer, String channelId) {
        ByteArrayInputStream stream = new ByteArrayInputStream(buffer.array());
        
        AisReader reader = AisReaders.createReaderFromInputStream(stream);
        reader.registerHandler(new Consumer<AisMessage>() {
            @Override
            public void accept(AisMessage aisMessage) {
                System.out.println("message id: " + aisMessage.getMsgId());
                GeoEvent event = createGeoEvent(aisMessage);
                geoEventListener.receive(event);
            }
        });
        reader.start();
        try {
            reader.join();
            
        } catch (InterruptedException ex) {
            log.error("buffer was interrupted", ex);
        } catch (BufferUnderflowException ex) {
            log.error("buffer underflow", ex);
        } finally {
            reader.stopReader();
            buffer.reset();
        }

    }

    @Override
    public void shutdown() {
        try {
            super.shutdown();
        } catch (Exception e) {
            log.fatal("Adaptor was not shutdown properly", e);
        }
    }

    private GeoEvent createGeoEvent(AisMessage aisMessage) {
        GeoEvent event;
        try {
            event = geoEventCreator.create(((AdapterDefinition) definition).getGeoEventDefinition("AisMessage").getGuid());
        } catch (MessagingException ex) {
            log.error("could not create AisEvent from xml", ex);
            return null;
        }
        try {
            event.setField("track_id", aisMessage.getMsgId());

            Position position = aisMessage.getValidPosition();
            int wkid = 4326; //WGS84
            event.setField("location", spatial.createPoint(position.getLatitude(), position.getLongitude(), wkid));
            event.setField("class", aisMessage.getClass());
            if (aisMessage.getSourceTag() != null) {
                IProprietarySourceTag sourceTag = aisMessage.getSourceTag();
                event.setField("timestamp", sourceTag.getTimestamp());
            }
            addMoreFields(event, aisMessage);

        } catch (FieldException ex) {
            log.error("Could not set a field on the GeoEvent.", ex);
        }

        return event;
    }

    @Override
    protected GeoEvent adapt(ByteBuffer bb, String string) {
        return null;
    }

    private void addMoreFields(GeoEvent event, AisMessage aisMessage) {
        // Handle AtoN message
        if (aisMessage instanceof AisMessage21) {
            AisMessage21 msg21 = (AisMessage21) aisMessage;
            log.info("AtoN name: " + msg21.getName());
        }
        // Handle position messages 1,2 and 3 (class A) by using their shared parent
        if (aisMessage instanceof AisPositionMessage) {
            AisPositionMessage posMessage = (AisPositionMessage) aisMessage;
            log.info("speed over ground: " + posMessage.getSog());
        }
        // Handle position messages 1,2,3 and 18 (class A and B)  
        if (aisMessage instanceof IVesselPositionMessage) {
            IVesselPositionMessage posMessage = (IVesselPositionMessage) aisMessage;
            log.info("course over ground: " + posMessage.getCog());
        }
        // Handle static reports for both class A and B vessels (msg 5 + 24)
        if (aisMessage instanceof AisStaticCommon) {
            AisStaticCommon staticMessage = (AisStaticCommon) aisMessage;
            log.info("vessel name: " + staticMessage.getName());
        }
    }
}
