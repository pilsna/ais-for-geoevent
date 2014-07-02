package dk.informi.geoevent.ais;

import com.esri.ges.adapter.AdapterDefinition;
import com.esri.ges.adapter.InboundAdapterBase;
import com.esri.ges.core.component.ComponentException;
import com.esri.ges.core.geoevent.FieldException;
import com.esri.ges.core.geoevent.GeoEvent;
import com.esri.ges.core.geoevent.GeoEventDefinition;
import com.esri.ges.manager.geoeventdefinition.GeoEventDefinitionManagerException;
import com.esri.ges.messaging.MessagingException;
import dk.dma.ais.reader.AisReader;
import java.nio.ByteBuffer;
import dk.dma.ais.reader.AisReaders;
import dk.dma.ais.message.AisMessage;
import dk.dma.ais.message.AisMessage21;
import dk.dma.ais.message.AisPositionMessage;
import dk.dma.ais.message.AisStaticCommon;
import dk.dma.ais.message.IVesselPositionMessage;
import dk.dma.enav.model.geometry.Position;
import java.nio.BufferUnderflowException;
import dk.dma.enav.util.function.Consumer;
import java.io.IOException;
import java.io.InputStream;
import java.nio.InvalidMarkException;
import java.util.Date;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class AisInboundAdapter extends InboundAdapterBase {

    private static final Log log = LogFactory.getLog(AisInboundAdapterService.class);

    public AisInboundAdapter(AdapterDefinition definition) throws ComponentException {
        super(definition);
        log.info("Created AisInboundAdapter.");
    }


    @Override
    protected GeoEvent adapt(ByteBuffer buffer, String string) {
        InputStream inputStream = newInputStream(buffer);
        
        AisReader reader = AisReaders.createReaderFromInputStream(inputStream);
        reader.registerHandler(new Consumer<AisMessage>() {
            @Override
            public void accept(AisMessage aisMessage) {
                GeoEvent event = createGeoEvent(aisMessage);
                if (event != null) {
                    geoEventListener.receive(event);
                } 
            }
        });
        reader.start();
        try {
            reader.join();

        } catch (InterruptedException ex) {
            log.error("buffer was interrupted", ex);
            reader.stopReader();
            buffer.reset();
        } catch (BufferUnderflowException ex) {
            log.error("buffer underflow", ex);
            reader.stopReader();
            buffer.reset();
        } catch (InvalidMarkException ex) {
            log.error("Invalid Mark", ex);
            reader.stopReader();
            buffer.reset();
        }

        return null;
    }
    // Returns an input stream for a ByteBuffer.
    // The read() methods use the relative ByteBuffer get() methods.
    public static InputStream newInputStream(final ByteBuffer buf) {
        return new InputStream() {
            @Override
            public synchronized int read() throws IOException {
                if (!buf.hasRemaining()) {
                    return -1;
                }
                return buf.get();
            }

            @Override
            public synchronized int read(byte[] bytes, int off, int len) throws IOException {
                // Read only what's left
                len = Math.min(len, buf.remaining());
                
                buf.get(bytes, off, len);
                
                if (len == 0) {
                    return -1;
                } else {
                    return len;
                }
            }
        };
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
        AdapterDefinition def = (AdapterDefinition) definition;
        GeoEventDefinition geoDef = def.getGeoEventDefinition("IGAisMessage");
        try {
            if (geoEventCreator.getGeoEventDefinitionManager().searchGeoEventDefinition(geoDef.getName(), geoDef.getOwner()) == null) {
              geoEventCreator.getGeoEventDefinitionManager().addGeoEventDefinition(geoDef);
            }
            event = geoEventCreator.create(geoDef.getName(), geoDef.getOwner());
                //event = geoEventCreator.create(((AdapterDefinition) definition).getGeoEventDefinition("IGAisMessage").getGuid());
        } catch (MessagingException ex) {
            log.error("Could not create IGAisMessage GeoEvent", ex);
            return null;
        } catch (GeoEventDefinitionManagerException ex) {
            log.error("Could not create IGAisMessage GeoEvent.", ex);
            return null;
        }
        try {
            event.setField("MID", aisMessage.getMsgId());
            event.setField("MMSI", aisMessage.getUserId());
            
            event.setField("CreationTime", new Date());
            
            addSubtypeFields(event, aisMessage);

        } catch (FieldException ex) {
            log.error("Could not set a field on the GeoEvent.", ex);
        }

        return event;
    }

    private void addSubtypeFields(GeoEvent event, AisMessage aisMessage) throws FieldException {
        // Handle AtoN message
        if (aisMessage instanceof AisMessage21) {
            AisMessage21 msg21 = (AisMessage21) aisMessage;
            log.info("AtoN name: " + msg21.getName());
        }
        // Handle position messages 1,2 and 3 (class A) by using their shared parent
        if (aisMessage instanceof AisPositionMessage) {
            AisPositionMessage posMessage = (AisPositionMessage) aisMessage;
            double speed = posMessage.getSog() / 10;
            event.setField("Speed", speed );
            log.debug("Speed: " + speed);
            Position position = posMessage.getValidPosition();
            int wkid = 4326; //WGS84
            if (position != null){
                event.setField("shape", spatial.createPoint(position.getLongitude(), position.getLatitude(), wkid));
            }
            double courseOverGround = posMessage.getCog() / 10;
            event.setField("CourseOverGround", courseOverGround);
            log.debug("CourseOverGround: " + courseOverGround);
            
        }
        // Handle position messages 1,2,3 and 18 (class A and B)  
        if (aisMessage instanceof IVesselPositionMessage) {
            IVesselPositionMessage posMessage = (IVesselPositionMessage) aisMessage;
            double courseOverGround = posMessage.getCog() / 10;
            event.setField("CourseOverGround", courseOverGround);
            log.debug("CourseOverGround: " + courseOverGround);
        }
        // Handle static reports for both class A and B vessels (msg 5 + 24)
        if (aisMessage instanceof AisStaticCommon) {
            AisStaticCommon staticMessage = (AisStaticCommon) aisMessage;
            event.setField("Vesselname", staticMessage.getName());
            log.debug("Vesselname: " + staticMessage.getName());
        }

    }
}
