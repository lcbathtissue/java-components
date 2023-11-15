package programmingtheiot.gda.connection.handlers;

import java.util.logging.Logger;

import org.eclipse.californium.core.CoapResource;
import org.eclipse.californium.core.coap.CoAP;
import org.eclipse.californium.core.coap.CoAP.ResponseCode;
import org.eclipse.californium.core.server.resources.CoapExchange;

import programmingtheiot.common.ConfigConst;
import programmingtheiot.common.ConfigUtil;
import programmingtheiot.common.IDataMessageListener;
import programmingtheiot.common.ResourceNameEnum;
import programmingtheiot.data.DataUtil;
import programmingtheiot.data.SystemPerformanceData;

/**
 * Shell representation of class for student implementation.
 *
 */
public class UpdateTelemetryResourceHandler extends CoapResource {
    // static

    private static final Logger _Logger =
            Logger.getLogger(GenericCoapResourceHandler.class.getName());

    // params
    private IDataMessageListener dataMsgListener = null;

    // constructors

    /**
     * Constructor.
     *
     * @param resource Basically, the path (or topic)
     */
    public UpdateTelemetryResourceHandler(ResourceNameEnum resource)
    {
        this(resource.getResourceName());
    }

    /**
     * Constructor.
     *
     * @param resourceName The name of the resource.
     */
    public UpdateTelemetryResourceHandler(String resourceName)
    {
        super(resourceName);
    }


    // public methods

    @Override
    public void handleDELETE(CoapExchange context)
    {
    }

    @Override
    public void handleGET(CoapExchange context)
    {
    }

    @Override
    public void handlePOST(CoapExchange context)
    {
    }
    @Override
    public void handlePUT(CoapExchange context)
    {
        CoAP.ResponseCode code = CoAP.ResponseCode.NOT_ACCEPTABLE;

        context.accept();

        if (this.dataMsgListener != null) {
            try {
                String jsonData = new String(context.getRequestPayload());

                SystemPerformanceData sysPerfData =
                        DataUtil.getInstance().jsonToSystemPerformanceData(jsonData);

                // TODO: Choose the following (but keep it idempotent!)
                //   1) Check MID to see if it’s repeated for some reason
                //      - optional, as the underlying lib should handle this
                //   2) Cache the previous update – is the PAYLOAD repeated?
                //   2) Delegate the data check to this.dataMsgListener

                this.dataMsgListener.handleSystemPerformanceMessage(
                        ResourceNameEnum.CDA_SYSTEM_PERF_MSG_RESOURCE, sysPerfData);

                code = CoAP.ResponseCode.CHANGED;
            } catch (Exception e) {
                _Logger.warning(
                        "Failed to handle PUT request. Message: " +
                                e.getMessage());

                code = CoAP.ResponseCode.BAD_REQUEST;
            }
        } else {
            _Logger.info(
                    "No callback listener for request. Ignoring PUT.");

            code = CoAP.ResponseCode.CONTINUE;
        }

        String msg =
                "Update system perf data request handled: " + super.getName();

        context.respond(code, msg);
    }

    public void setDataMessageListener(IDataMessageListener listener)
    {
        if (listener != null) {
            this.dataMsgListener = listener;
        }
    }

}
