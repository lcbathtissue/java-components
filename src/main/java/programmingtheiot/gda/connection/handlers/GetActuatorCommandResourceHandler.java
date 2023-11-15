package programmingtheiot.gda.connection.handlers;

import java.util.logging.Logger;

import org.eclipse.californium.core.CoapResource;
import org.eclipse.californium.core.coap.CoAP;
import org.eclipse.californium.core.coap.CoAP.ResponseCode;
import org.eclipse.californium.core.server.resources.CoapExchange;

import programmingtheiot.common.*;
import programmingtheiot.data.ActuatorData;
import programmingtheiot.data.DataUtil;
import programmingtheiot.data.SystemPerformanceData;

/**
 * Shell representation of class for student implementation.
 *
 */
public class GetActuatorCommandResourceHandler extends CoapResource implements IActuatorDataListener {
    // static


    private static final Logger _Logger =
            Logger.getLogger(GetActuatorCommandResourceHandler.class.getName());

    // params
    private IDataMessageListener dataMsgListener = null;

    private ActuatorData actuatorData = null;

    // constructors

    /**
     * Constructor.
     *
     * @param resource Basically, the path (or topic)
     */
    public GetActuatorCommandResourceHandler(ResourceNameEnum resource)
    {
        super(resource.getResourceName());

        // set the resource to be observable
        super.setObservable(true);
    }



    /**
     * Constructor.
     *
     * @param resourceName The name of the resource.
     */
    public GetActuatorCommandResourceHandler(String resourceName)
    {
        super(resourceName);
    }

    // public methods

    public boolean onActuatorDataUpdate(ActuatorData data)
    {
        if (data != null && this.actuatorData != null) {
            this.actuatorData.updateData(data);

            // notify all connected clients
            super.changed();

            _Logger.fine("Actuator data updated for URI: " + super.getURI() + ": Data value = " + this.actuatorData.getValue());

            return true;
        }

        return false;
    }

    @Override
    public void handleDELETE(CoapExchange context)
    {
    }

    @Override
    public void handleGET(CoapExchange context)
    {
        // TODO: validate 'context'

        // accept the request
        context.accept();

        // TODO: convert the locally stored ActuatorData to JSON using DataUtil
        String jsonData = DataUtil.getInstance().actuatorDataToJson(this.actuatorData);

        // TODO: generate a response message, set the content type, and set the response code

        // send an appropriate response
        context.respond(ResponseCode.CONTENT, jsonData);
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
