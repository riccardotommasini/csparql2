package it.polimi.jasper.spe.esper;


import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.common.client.util.EventTypeBusModifier;
import com.espertech.esper.runtime.client.EPDeploymentService;
import com.espertech.esper.runtime.client.EPEventService;
import com.espertech.esper.runtime.client.EPRuntime;
import com.espertech.esper.runtime.client.EPRuntimeProvider;

import java.util.HashMap;


public class RuntimeManager {

    private static EPRuntime cep;
    private static EPDeploymentService cepAdm;
    private static EPEventService cepRT;

    public static EPRuntime getCEP() {
        if (cep == null) {
            Configuration cep_config = new Configuration();
            cep_config.getRuntime().getThreading().setInternalTimerEnabled(false);
            cep_config.getCompiler().getByteCode().setAccessModifiersPublic();
            cep_config.getCommon().addEventType("TStream", new HashMap<>());
            cep_config.getCompiler().getByteCode().setBusModifierEventType(EventTypeBusModifier.BUS);

            String canonicalName = RuntimeManager.class.getCanonicalName();
            cep = EPRuntimeProvider.getRuntime(canonicalName, cep_config);
        }
        return cep;
    }


    public static EPDeploymentService getAdmin() {
        if (cepAdm == null) {
            cepAdm = cep.getDeploymentService();
        }
        return cepAdm;
    }

    public static EPEventService getEPRuntime() {
        if (cepRT == null) {
            cepRT = cep.getEventService();
        }
        return cepRT;
    }

    public static RuntimeManager getInstance() {
        return new RuntimeManager();
    }

    public static Configuration getRuntimeConfiguration() {
        return cep.getConfigurationDeepCopy();
    }
}
