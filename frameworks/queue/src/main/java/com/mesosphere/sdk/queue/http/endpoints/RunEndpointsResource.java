package com.mesosphere.sdk.queue.http.endpoints;

import java.util.Optional;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;

import com.mesosphere.sdk.http.queries.EndpointsQueries;
import com.mesosphere.sdk.queue.http.types.RunInfoProvider;
import com.mesosphere.sdk.state.StateStore;

/**
 * A read-only API for accessing information about how to connect to the service.
 */
@Path("/v1/run")
public class RunEndpointsResource {

    private final String frameworkName;
    private final RunInfoProvider runInfoProvider;

    /**
     * Creates a new instance which retrieves task/pod state from the provided {@link StateStore},
     * using the provided {@code serviceName} for endpoint paths.
     */
    public RunEndpointsResource(String frameworkName, RunInfoProvider runInfoProvider) {
        this.frameworkName = frameworkName;
        this.runInfoProvider = runInfoProvider;
    }

    /**
     * @see EndpointsQueries
     */
    @Path("{runName}/endpoints")
    @GET
    public Response getEndpoints(@PathParam("runName") String runName) {
        Optional<StateStore> stateStore = runInfoProvider.getStateStore(runName);
        if (!stateStore.isPresent()) {
            return QueueResponseUtils.runNotFoundResponse(runName);
        }
        return EndpointsQueries.getEndpoints(
                stateStore.get(), frameworkName, runInfoProvider.getCustomEndpoints(runName));
    }

    /**
     * @see EndpointsQueries
     */
    @Path("{runName}/endpoints/{endpointName}")
    @GET
    public Response getEndpoint(@PathParam("runName") String runName, @PathParam("endpointName") String endpointName) {
        Optional<StateStore> stateStore = runInfoProvider.getStateStore(runName);
        if (!stateStore.isPresent()) {
            return QueueResponseUtils.runNotFoundResponse(runName);
        }
        return EndpointsQueries.getEndpoint(
                stateStore.get(), frameworkName, runInfoProvider.getCustomEndpoints(runName), endpointName);
    }

}
