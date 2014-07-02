package dk.informi.geoevent.ais;

import com.esri.ges.adapter.Adapter;
import com.esri.ges.adapter.AdapterServiceBase;
import com.esri.ges.adapter.util.XmlAdapterDefinition;
import com.esri.ges.core.component.ComponentException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author nilsr
 */
public class AisInboundAdapterService extends AdapterServiceBase {
    private static final Log log = LogFactory.getLog(AisInboundAdapterService.class);
    public AisInboundAdapterService() {
        XmlAdapterDefinition xmlAdapterDefinition = new XmlAdapterDefinition(getResourceAsStream("aisinbound-adapter-definition.xml"));
        log.info("Creating AisInboundAdapterServiceDefinition from xml file");
        definition = xmlAdapterDefinition;
    }

    @Override
    public Adapter createAdapter() throws ComponentException {
        AisInboundAdapter adapter = null;
        try {
            return new AisInboundAdapter(definition);
        } catch (ComponentException exception){
            log.error(exception);
            throw (ComponentException) exception;
        }
    }
}
