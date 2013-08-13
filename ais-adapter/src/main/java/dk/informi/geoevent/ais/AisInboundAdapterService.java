package dk.informi.geoevent.ais;

import com.esri.ges.adapter.Adapter;
import com.esri.ges.adapter.AdapterServiceBase;
import com.esri.ges.core.component.ComponentException;

/**
 *
 * @author nilsr
 */
public class AisInboundAdapterService extends AdapterServiceBase {

    public AisInboundAdapterService() {
        definition = new AisInboundAdapterDefinition();
    }

    @Override
    public Adapter createAdapter() throws ComponentException {
        return new AisInboundAdapter(definition);
    }
}
