package dk.informi.geoevent.ais;

import com.esri.ges.adapter.Adapter;
import com.esri.ges.adapter.AdapterServiceBase;
import com.esri.ges.adapter.util.XmlAdapterDefinition;
import com.esri.ges.core.component.ComponentException;

/**
 *
 * @author nilsr
 */
public class AisInboundAdapterService extends AdapterServiceBase {

    public AisInboundAdapterService() {
        XmlAdapterDefinition xmlAdapterDefinition = new XmlAdapterDefinition(getResourceAsStream("aisinbound-adapter-definition.xml"));
        definition = xmlAdapterDefinition;
    }

    @Override
    public Adapter createAdapter() throws ComponentException {
        return new AisInboundAdapter(definition);
    }
}
